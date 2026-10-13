import difflib
import re
from dataclasses import dataclass, field
from pathlib import PurePosixPath

import sqlglot
from sqlglot.errors import SqlglotError

from pipeline_doctor.agent.context import FailedNode, IncidentContext
from pipeline_doctor.agent.fixes import build_revert_edits
from pipeline_doctor.agent.reasoning.base import Assessment, Hypothesis, Investigation
from pipeline_doctor.dbt_tools.artifacts import ResourceType
from pipeline_doctor.diagnostics.checks import CheckKind, CheckRequest, CheckResult, ValueFormat
from pipeline_doctor.diagnostics.schema_drift import SchemaDrift
from pipeline_doctor.incidents.models import (
    Culprit,
    Diagnosis,
    FileEdit,
    FixKind,
    FixProposal,
    RootCauseCategory,
)
from pipeline_doctor.vcs.history import CommitChange

_MISSING_COLUMN = re.compile(
    r'(?:Referenced column "(\w+)" not found|column "?(\w+)"? (?:not found|does not exist)|invalid identifier \'(?:\w+\.)?(\w+)\')',
    re.IGNORECASE,
)
_CONVERSION_ERROR = re.compile(r"(conversion error|could not convert|is not recognized|invalid date|cannot cast)", re.I)
_SYNTAX_ERROR = re.compile(r"(parser error|syntax error|compilation error|depends on a node named)", re.IGNORECASE)
_ACCEPTED_VALUES_LINE = re.compile(r"values:\s*\[(?P<items>[^\]]*)\]")
_JOIN_CHANGE = re.compile(r"^[+-].*(\bjoin\b|\bref\(|\bgroup by\b|\bdistinct\b)", re.IGNORECASE | re.MULTILINE)
_CONFIDENT = 0.6
_SIMILAR_INCIDENT_THRESHOLD = 0.6


@dataclass
class _Score:
    value: float = 0.0
    reasons: list[str] = field(default_factory=list)

    def add(self, amount: float, reason: str) -> None:
        self.value += amount
        self.reasons.append(reason)


class HeuristicReasoner:
    """Deterministic rule-based reasoner: the offline baseline, and the fallback when an LLM is unavailable."""

    def __init__(self, dialect: str = "duckdb") -> None:
        self._dialect = dialect

    @property
    def name(self) -> str:
        return "heuristic"

    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]:
        context = investigation.context
        if investigation.rounds > 0:
            return self._broaden(context)
        primary = context.primary
        if primary is None:
            return self._hypothesize_from_log(context)
        if primary.resource_type is ResourceType.SOURCE:
            return [self._stale_hypothesis(primary)]
        drift_hypotheses = self._hypothesize_schema_drift(context)
        if primary.resource_type is ResourceType.TEST:
            return drift_hypotheses + self._hypothesize_test_failure(primary) + self._code_hypotheses(context, primary)
        return drift_hypotheses + self._code_hypotheses(context, primary)

    def assess(self, investigation: Investigation) -> Assessment:
        scored = [(self._score(hypothesis, investigation), hypothesis) for hypothesis in investigation.hypotheses]
        if not scored:
            return Assessment(ready=False, diagnosis=self._unknown(investigation.context))
        score, best = max(scored, key=lambda item: item[0].value)
        confidence = round(min(score.value, 0.99), 2)
        diagnosis = Diagnosis(
            category=best.category,
            confidence=confidence,
            culprit=best.culprit,
            technical_summary=self._technical_summary(best, investigation.context),
            plain_summary=self._plain_summary(best, investigation.context),
            evidence=score.reasons,
        )
        return Assessment(ready=confidence >= _CONFIDENT, diagnosis=diagnosis)

    def propose_fix(self, investigation: Investigation, feedback: str) -> FixProposal:
        diagnosis = investigation.diagnosis
        if diagnosis is None:
            return _operational("Investigate manually", "No diagnosis was produced.", ["Triage the failure by hand."])
        context = investigation.context
        builders = {
            RootCauseCategory.SOURCE_COLUMN_RENAMED: self._fix_renamed_column,
            RootCauseCategory.SOURCE_TYPE_CHANGED: self._fix_type_change,
            RootCauseCategory.SOURCE_DUPLICATE_ROWS: self._fix_duplicates,
            RootCauseCategory.SOURCE_NEW_CATEGORY: self._fix_new_category,
            RootCauseCategory.CODE_SQL_ERROR: self._fix_by_revert,
            RootCauseCategory.CODE_JOIN_FANOUT: self._fix_by_revert,
            RootCauseCategory.CODE_LOGIC_REGRESSION: self._fix_by_revert,
        }
        builder = builders.get(diagnosis.category)
        fix = builder(investigation, diagnosis) if builder else None
        return fix or _operational_fix_for(diagnosis, context)

    # Hypothesis generation

    def _hypothesize_from_log(self, context: IncidentContext) -> list[Hypothesis]:
        sql_commits = [commit for commit in context.commits if commit.changes_lineage_sql]
        return [
            Hypothesis(
                category=RootCauseCategory.CODE_SQL_ERROR,
                culprit=Culprit(relation=_model_name(commit.lineage_files), commit_sha=commit.sha),
                rationale="dbt failed before producing run results and a recent commit changed SQL.",
            )
            for commit in sql_commits
        ]

    def _stale_hypothesis(self, primary: FailedNode) -> Hypothesis:
        return Hypothesis(
            category=RootCauseCategory.SOURCE_STALE,
            culprit=Culprit(relation=primary.name),
            rationale="The source freshness check failed.",
            checks=[
                CheckRequest(kind=CheckKind.FRESHNESS, relation=primary.name),
                CheckRequest(kind=CheckKind.VOLUME_BY_LOAD_DAY, relation=primary.name),
            ],
        )

    def _hypothesize_schema_drift(self, context: IncidentContext) -> list[Hypothesis]:
        hypotheses = []
        for drift in context.schema_drift:
            if drift.missing_columns:
                hypotheses.append(
                    Hypothesis(
                        category=RootCauseCategory.SOURCE_COLUMN_RENAMED,
                        culprit=Culprit(relation=drift.source, column=drift.missing_columns[0]),
                        rationale=drift.describe(),
                        checks=[
                            CheckRequest(kind=CheckKind.COLUMN_PROFILE, relation=drift.source, column=column)
                            for column in drift.added_columns[:2]
                        ],
                    )
                )
            hypotheses.extend(
                Hypothesis(
                    category=RootCauseCategory.SOURCE_TYPE_CHANGED,
                    culprit=Culprit(relation=drift.source, column=change.column),
                    rationale=drift.describe(),
                    checks=[CheckRequest(kind=CheckKind.COLUMN_PROFILE, relation=drift.source, column=change.column)],
                )
                for change in drift.type_changes
            )
        return hypotheses

    def _hypothesize_test_failure(self, primary: FailedNode) -> list[Hypothesis]:
        relation, column = primary.tested_relation or "", primary.tested_column or ""
        source, source_column = primary.traced_source or relation, primary.traced_source_column or column
        sample_failures = (
            [CheckRequest(kind=CheckKind.CUSTOM_SQL, sql=primary.compiled_code, rationale="Sample failing rows.")]
            if primary.compiled_code
            else []
        )
        match primary.test_name:
            case "not_null":
                return [
                    Hypothesis(
                        category=RootCauseCategory.SOURCE_NULL_SPIKE,
                        culprit=Culprit(relation=source, column=source_column),
                        rationale=f"not_null failed on {relation}.{column}, read from {source}.{source_column}.",
                        checks=[
                            CheckRequest(kind=CheckKind.NULL_RATE_BY_LOAD_DAY, relation=source, column=source_column),
                            *sample_failures,
                        ],
                    )
                ]
            case "unique":
                return [
                    Hypothesis(
                        category=RootCauseCategory.SOURCE_DUPLICATE_ROWS,
                        culprit=Culprit(relation=source, column=source_column),
                        rationale=f"unique failed on {relation}.{column}, keyed by {source}.{source_column}.",
                        checks=[
                            CheckRequest(kind=CheckKind.DUPLICATE_KEYS, relation=source, column=source_column),
                            CheckRequest(kind=CheckKind.DUPLICATE_KEYS, relation=relation, column=column),
                        ],
                    )
                ]
            case "accepted_values":
                accepted = [str(value) for value in primary.test_kwargs.get("values", [])]
                return [
                    Hypothesis(
                        category=RootCauseCategory.SOURCE_NEW_CATEGORY,
                        culprit=Culprit(relation=source, column=source_column),
                        rationale=f"accepted_values failed on {relation}.{column}.",
                        checks=[
                            CheckRequest(
                                kind=CheckKind.UNEXPECTED_VALUES,
                                relation=source,
                                column=source_column,
                                accepted_values=accepted,
                            )
                        ],
                    )
                ]
            case "relationships":
                return [self._missing_parent_hypothesis(primary)]
            case _:
                return [
                    Hypothesis(
                        category=RootCauseCategory.UNKNOWN,
                        culprit=Culprit(relation=relation or primary.name),
                        rationale="Custom test failed; inspect the failing rows.",
                        checks=sample_failures,
                    )
                ]

    def _missing_parent_hypothesis(self, primary: FailedNode) -> Hypothesis:
        parent, parent_field = primary.parent_relation or "", primary.parent_column or ""
        parent_source = primary.parent_traced_source or parent
        return Hypothesis(
            category=RootCauseCategory.SOURCE_MISSING_ROWS,
            culprit=Culprit(relation=parent_source),
            rationale=f"relationships failed: {primary.tested_relation}.{primary.tested_column} -> {parent}.{parent_field}.",
            checks=[
                CheckRequest(
                    kind=CheckKind.ORPHAN_KEYS,
                    relation=primary.tested_relation or "",
                    column=primary.tested_column or "",
                    parent_relation=parent,
                    parent_column=parent_field,
                ),
                CheckRequest(kind=CheckKind.VOLUME_BY_LOAD_DAY, relation=parent_source),
            ],
        )

    def _code_hypotheses(self, context: IncidentContext, primary: FailedNode) -> list[Hypothesis]:
        hypotheses = []
        for commit in context.commits:
            if not commit.changes_lineage_sql:
                continue
            if primary.resource_type is ResourceType.MODEL:
                category = RootCauseCategory.CODE_SQL_ERROR
            elif primary.test_name == "unique":
                category = RootCauseCategory.CODE_JOIN_FANOUT
            else:
                category = RootCauseCategory.CODE_LOGIC_REGRESSION
            hypotheses.append(
                Hypothesis(
                    category=category,
                    culprit=Culprit(relation=_model_name(commit.lineage_files), commit_sha=commit.sha),
                    rationale=f"Commit {commit.sha[:8]} ({commit.message!r}) changed SQL in the failing lineage.",
                )
            )
        return hypotheses

    def _broaden(self, context: IncidentContext) -> list[Hypothesis]:
        return [
            Hypothesis(
                category=RootCauseCategory.SOURCE_STALE,
                culprit=Culprit(relation=source),
                rationale="Broadening: check whether any upstream source stopped loading.",
                checks=[
                    CheckRequest(kind=CheckKind.FRESHNESS, relation=source),
                    CheckRequest(kind=CheckKind.VOLUME_BY_LOAD_DAY, relation=source),
                ],
            )
            for source in context.upstream_sources
        ]

    # Scoring

    def _score(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        scorers = {
            RootCauseCategory.SOURCE_STALE: self._score_stale,
            RootCauseCategory.SOURCE_COLUMN_RENAMED: self._score_renamed,
            RootCauseCategory.SOURCE_TYPE_CHANGED: self._score_type_change,
            RootCauseCategory.SOURCE_NULL_SPIKE: self._score_null_spike,
            RootCauseCategory.SOURCE_DUPLICATE_ROWS: self._score_duplicates,
            RootCauseCategory.SOURCE_NEW_CATEGORY: self._score_new_category,
            RootCauseCategory.SOURCE_MISSING_ROWS: self._score_missing_rows,
            RootCauseCategory.CODE_SQL_ERROR: self._score_sql_error,
            RootCauseCategory.CODE_JOIN_FANOUT: self._score_fanout,
            RootCauseCategory.CODE_LOGIC_REGRESSION: self._score_logic_regression,
        }
        scorer = scorers.get(hypothesis.category)
        score = scorer(hypothesis, investigation) if scorer else _Score()
        similar = [
            item
            for item in investigation.context.similar_incidents
            if item.incident.category == hypothesis.category and item.similarity >= _SIMILAR_INCIDENT_THRESHOLD
        ]
        if similar:
            past = similar[0].incident
            score.add(0.05, f"matches past incident {past.incident_id} ({past.resolution})")
        return score

    def _score_stale(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score, primary = _Score(), investigation.context.primary
        if primary and primary.resource_type is ResourceType.SOURCE:
            score.add(0.5, f"dbt source freshness failed for {hypothesis.culprit.relation}")
        for result in _of_kind(_evidence_for(investigation, hypothesis), CheckKind.FRESHNESS):
            hours = result.metrics.get("hours_since_last_load")
            if isinstance(hours, int | float) and hours > 36:
                score.add(0.45, f"{hypothesis.culprit.relation} last loaded {hours} hours ago")
        return score

    def _score_renamed(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score, context = _Score(), investigation.context
        drift = _drift_for(context, hypothesis.culprit.relation)
        missing_column = _missing_column(_failure_message(context))
        if drift and hypothesis.culprit.column in drift.missing_columns:
            score.add(0.5, f"schema drift: {drift.describe()}")
        if missing_column and missing_column == hypothesis.culprit.column:
            score.add(0.4, f"the model error references the missing column `{missing_column}`")
        if drift and drift.added_columns:
            score.add(0.05, f"candidate replacement columns: {drift.added_columns}")
        return score

    def _score_type_change(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score, context = _Score(), investigation.context
        drift = _drift_for(context, hypothesis.culprit.relation)
        if drift and any(change.column == hypothesis.culprit.column for change in drift.type_changes):
            score.add(0.5, f"schema drift: {drift.describe()}")
        if _CONVERSION_ERROR.search(_failure_message(context)):
            score.add(0.4, "the failure is a type conversion error")
        for result in _of_kind(_evidence_for(investigation, hypothesis), CheckKind.COLUMN_PROFILE):
            score.add(0.05, result.summary)
        return score

    def _score_null_spike(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score = _Score()
        for result in _of_kind(_evidence_for(investigation, hypothesis), CheckKind.NULL_RATE_BY_LOAD_DAY):
            if result.metrics.get("spike"):
                score.add(0.9, result.summary)
            elif _metric_count(result, "total_null_rows") > 0:
                score.add(0.25, result.summary)
        return score

    def _score_duplicates(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score = _Score()
        for result in _of_kind(investigation.evidence, CheckKind.DUPLICATE_KEYS):
            if result.request.relation != hypothesis.culprit.relation:
                continue
            if _metric_count(result, "duplicated_keys") > 0:
                score.add(0.8, result.summary)
                if result.metrics.get("duplicates_span_multiple_loads"):
                    score.add(0.1, "the duplicate copies arrived in separate load batches")
        return score

    def _score_new_category(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score = _Score()
        for result in _of_kind(_evidence_for(investigation, hypothesis), CheckKind.UNEXPECTED_VALUES):
            if result.metrics.get("unexpected_values"):
                score.add(0.9, result.summary)
        return score

    def _score_missing_rows(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        score = _Score()
        for result in _of_kind(investigation.evidence, CheckKind.ORPHAN_KEYS):
            if _metric_count(result, "orphan_keys") > 0:
                score.add(0.55, result.summary)
                if result.metrics.get("orphans_within_parent_key_range"):
                    score.add(0.3, "orphans sit inside the parent key range, so parent rows went missing")
        return score

    def _score_sql_error(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        context = investigation.context
        commit = _commit(context, hypothesis.culprit.commit_sha)
        if commit is None:
            return _Score()
        score = _score_commit(commit)
        message = _failure_message(context)
        missing_column = _missing_column(message)
        added_lines = "\n".join(line for line in commit.diff.splitlines() if line.startswith("+"))
        if missing_column and re.search(rf"\b{re.escape(missing_column)}\b", added_lines):
            score.add(0.5, f"the commit introduced the unknown identifier `{missing_column}`")
        elif _SYNTAX_ERROR.search(message):
            score.add(0.45, "dbt reported a syntax/compilation error right after this change")
        if missing_column and any(missing_column in drift.missing_columns for drift in context.schema_drift):
            score.add(-0.4, f"`{missing_column}` disappeared from a source, which points upstream instead")
        return score

    def _score_fanout(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        commit = _commit(investigation.context, hypothesis.culprit.commit_sha)
        if commit is None:
            return _Score()
        score = _score_commit(commit)
        if _JOIN_CHANGE.search(commit.diff):
            score.add(0.2, "the diff changes joins, refs, or aggregation")
        source_checks = [
            result
            for result in _of_kind(investigation.evidence, CheckKind.DUPLICATE_KEYS)
            if "." in result.request.relation
        ]
        if source_checks and all(_metric_count(result, "duplicated_keys") == 0 for result in source_checks):
            score.add(0.3, "source keys are unique, so the duplicates are created inside the model")
        return score

    def _score_logic_regression(self, hypothesis: Hypothesis, investigation: Investigation) -> _Score:
        commit = _commit(investigation.context, hypothesis.culprit.commit_sha)
        if commit is None:
            return _Score()
        score = _score_commit(commit)
        data_explained = any(
            result.metrics.get("spike") or result.metrics.get("unexpected_values") for result in investigation.evidence
        )
        if not data_explained:
            score.add(0.35, "no data anomaly explains the failure, so the logic change is the likely cause")
        return score

    # Explanations

    def _technical_summary(self, hypothesis: Hypothesis, context: IncidentContext) -> str:
        failing = context.primary.describe() if context.primary else "dbt failed before producing run results"
        return f"{failing}. Most likely cause: {hypothesis.category} on {hypothesis.culprit.describe()}. {hypothesis.rationale}"

    def _plain_summary(self, hypothesis: Hypothesis, context: IncidentContext) -> str:
        culprit = hypothesis.culprit
        assets = _assets_sentence(context.downstream_assets)
        templates = {
            RootCauseCategory.SOURCE_COLUMN_RENAMED: f"The system that sends us {culprit.relation} renamed or removed the `{culprit.column}` field, so our pipeline could not read it.",
            RootCauseCategory.SOURCE_TYPE_CHANGED: f"The system that sends us {culprit.relation} changed the format of `{culprit.column}`, so our pipeline could not interpret it.",
            RootCauseCategory.SOURCE_NULL_SPIKE: f"Recent {culprit.relation} data arrived with `{culprit.column}` missing on many rows, which our quality checks block.",
            RootCauseCategory.SOURCE_DUPLICATE_ROWS: f"The latest {culprit.relation} load was delivered twice, so records were duplicated and our quality checks stopped the pipeline.",
            RootCauseCategory.SOURCE_STALE: f"No new {culprit.relation} data has arrived recently: the upstream feed appears to have stopped.",
            RootCauseCategory.SOURCE_NEW_CATEGORY: f"{culprit.relation} started using a new `{culprit.column}` value we do not recognise yet, so reports could misclassify it.",
            RootCauseCategory.SOURCE_MISSING_ROWS: f"Some {culprit.relation} records are missing, so other records point to entries that do not exist.",
            RootCauseCategory.CODE_SQL_ERROR: f"A recent code change to {culprit.relation} contains an error, so the pipeline could not run it.",
            RootCauseCategory.CODE_JOIN_FANOUT: f"A recent code change to {culprit.relation} counts some records more than once, which would inflate totals.",
            RootCauseCategory.CODE_LOGIC_REGRESSION: f"A recent code change to {culprit.relation} altered a calculation, producing values our checks flag as wrong.",
        }
        return f"{templates.get(hypothesis.category, 'The cause is not yet clear.')} {assets}"

    def _unknown(self, context: IncidentContext) -> Diagnosis:
        relation = context.primary.tested_relation or context.primary.name if context.primary else "unknown"
        return Diagnosis(
            category=RootCauseCategory.UNKNOWN,
            confidence=0.1,
            culprit=Culprit(relation=relation),
            technical_summary="No hypothesis could be formed from the gathered context.",
            plain_summary=f"We could not determine the cause automatically. {_assets_sentence(context.downstream_assets)}",
        )

    # Fixes

    def _fix_renamed_column(self, investigation: Investigation, diagnosis: Diagnosis) -> FixProposal | None:
        context, culprit = investigation.context, diagnosis.culprit
        drift = _drift_for(context, culprit.relation)
        if drift is None or not drift.added_columns or not culprit.column:
            return None
        candidates = [column for column in drift.added_columns if not column.startswith("_")] or drift.added_columns
        new_name = (difflib.get_close_matches(culprit.column, candidates, n=1, cutoff=0.0) or candidates)[0]
        edit = self._edit_source_select(context, culprit.relation, f"select * rename ({new_name} as {culprit.column})")
        if edit is None:
            return None
        return FixProposal(
            kind=FixKind.SCHEMA_PATCH,
            title=f"Map renamed column {culprit.relation}.{new_name} back to {culprit.column}",
            rationale=(
                f"Upstream renamed `{culprit.column}` to `{new_name}`. Renaming it back where the source is read keeps "
                "every downstream model unchanged. Update the source declaration once the rename is confirmed permanent."
            ),
            edits=[edit],
        )

    def _fix_type_change(self, investigation: Investigation, diagnosis: Diagnosis) -> FixProposal | None:
        context, culprit = investigation.context, diagnosis.culprit
        drift = _drift_for(context, culprit.relation)
        change = next((item for item in (drift.type_changes if drift else []) if item.column == culprit.column), None)
        profile = next(
            (
                result
                for result in _of_kind(investigation.evidence, CheckKind.COLUMN_PROFILE)
                if result.request.relation == culprit.relation and result.request.column == culprit.column
            ),
            None,
        )
        if change is None or profile is None or culprit.column is None:
            return None
        expression = _parse_expression(culprit.column, str(profile.metrics.get("value_format")), change.declared_type)
        if expression is None:
            return None
        expression = self._transpile(expression)
        edit = self._edit_source_select(
            context, culprit.relation, f"select * replace ({expression} as {culprit.column})"
        )
        if edit is None:
            return None
        return FixProposal(
            kind=FixKind.SCHEMA_PATCH,
            title=f"Parse {culprit.relation}.{culprit.column} from its new {change.actual_type} format",
            rationale=(
                f"`{culprit.column}` now arrives as {change.actual_type} formatted like {profile.metrics.get('sample_values')}. "
                f"Parsing it back to {change.declared_type} at the source boundary restores the declared contract."
            ),
            edits=[edit],
        )

    def _fix_duplicates(self, investigation: Investigation, diagnosis: Diagnosis) -> FixProposal | None:
        context, culprit = investigation.context, diagnosis.culprit
        loaded_at = context.source_loaded_at_columns.get(culprit.relation) or "_loaded_at"
        source_name, table = culprit.relation.split(".", 1)
        file_path = self._consumer_file(context, culprit.relation)
        if file_path is None or not culprit.column:
            return None
        match = re.search(_source_select_pattern(source_name, table), context.project_files[file_path])
        if match is None:
            return None
        updated = f"{match.group(0)}\n    qualify row_number() over (partition by {culprit.column} order by {loaded_at} desc) = 1"
        return FixProposal(
            kind=FixKind.MODEL_CHANGE,
            title=f"Deduplicate {culprit.relation} on {culprit.column}",
            rationale=(
                f"{culprit.relation} received replayed rows. Keeping the latest copy per `{culprit.column}` makes the "
                "staging model idempotent to re-delivered batches. Also ask the ingestion owner why the batch was replayed."
            ),
            edits=[FileEdit(path=file_path, original=match.group(0), updated=updated)],
        )

    def _fix_new_category(self, investigation: Investigation, diagnosis: Diagnosis) -> FixProposal | None:
        context = investigation.context
        primary = context.primary
        values = next(
            (
                result.metrics.get("unexpected_values")
                for result in _of_kind(investigation.evidence, CheckKind.UNEXPECTED_VALUES)
                if result.request.relation == diagnosis.culprit.relation
                and result.request.column == diagnosis.culprit.column
            ),
            None,
        )
        if (
            primary is None
            or not isinstance(values, list)
            or not values
            or primary.file_path not in context.project_files
        ):
            return None
        accepted = [str(value) for value in primary.test_kwargs.get("values", [])]
        content = context.project_files[primary.file_path]
        for match in _ACCEPTED_VALUES_LINE.finditer(content):
            items = [item.strip().strip("'\"") for item in match.group("items").split(",") if item.strip()]
            if items != accepted:
                continue
            new_items = ", ".join(f"'{item}'" for item in [*accepted, *values])
            return FixProposal(
                kind=FixKind.TEST_CHANGE,
                title=f"Accept new {diagnosis.culprit.column} values {values}",
                rationale=(
                    f"The source now emits {values}. If these are legitimate business states, extend the accepted set; "
                    "confirm with the source owner and check whether downstream models need a mapping."
                ),
                edits=[FileEdit(path=primary.file_path, original=match.group(0), updated=f"values: [{new_items}]")],
            )
        return None

    def _fix_by_revert(self, investigation: Investigation, diagnosis: Diagnosis) -> FixProposal | None:
        context = investigation.context
        commit = _commit(context, diagnosis.culprit.commit_sha)
        if commit is None:
            return None
        edits = []
        for path in commit.lineage_files:
            before = context.pre_change_files.get(f"{commit.sha}:{path}")
            committed = context.committed_files.get(f"{commit.sha}:{path}")
            if before is None or committed is None or not path.endswith(".sql"):
                continue
            edits.extend(build_revert_edits(path, committed, before))
        if not edits or any(not edit.original for edit in edits):
            return None
        return FixProposal(
            kind=FixKind.MODEL_CHANGE,
            title=f"Revert {commit.sha[:8]} in {', '.join(edit.path for edit in edits)}",
            rationale=(
                f"Commit {commit.sha[:8]} ({commit.message!r}) by {commit.author} introduced the failure. Reverting the "
                "commit's hunks restores the last green behaviour and keeps later edits; re-apply the intended change "
                "with a test that covers this case."
            ),
            edits=edits,
        )

    def _edit_source_select(self, context: IncidentContext, relation: str, replacement_select: str) -> FileEdit | None:
        file_path = self._consumer_file(context, relation)
        if file_path is None or "." not in relation:
            return None
        source_name, table = relation.split(".", 1)
        match = re.search(_source_select_pattern(source_name, table), context.project_files[file_path])
        if match is None:
            return None
        updated = match.group(0).replace("select *", replacement_select, 1)
        return FileEdit(path=file_path, original=match.group(0), updated=updated)

    def _consumer_file(self, context: IncidentContext, relation: str) -> str | None:
        files = [path for path in context.source_consumer_files.get(relation, []) if path in context.project_files]
        return files[0] if files else None

    def _transpile(self, expression: str) -> str:
        try:
            return sqlglot.transpile(expression, read="duckdb", write=self._dialect)[0]
        except SqlglotError:
            return expression


def _source_select_pattern(source: str, table: str) -> str:
    return rf"select \* from \{{\{{\s*source\(\s*'{re.escape(source)}'\s*,\s*'{re.escape(table)}'\s*\)\s*\}}\}}"


def _operational(title: str, rationale: str, steps: list[str]) -> FixProposal:
    return FixProposal(kind=FixKind.OPERATIONAL, title=title, rationale=rationale, operational_steps=steps)


def _operational_fix_for(diagnosis: Diagnosis, context: IncidentContext) -> FixProposal:
    culprit = diagnosis.culprit
    steps = {
        RootCauseCategory.SOURCE_STALE: [
            f"Page the owner of the {culprit.relation} ingestion job; check its scheduler and credentials.",
            "Once data lands, re-run the DAG from the freshness task.",
            "Keep the freshness check as-is; it is doing its job.",
        ],
        RootCauseCategory.SOURCE_NULL_SPIKE: [
            f"Ask the {culprit.relation} source owner why `{culprit.column}` is null in recent loads.",
            "Request a corrected re-delivery of the affected batches, then re-run the DAG.",
            f"If the nulls are expected going forward, add a quarantine filter in the staging model for {culprit.relation}.",
        ],
        RootCauseCategory.SOURCE_MISSING_ROWS: [
            f"Ask the {culprit.relation} owner to backfill the missing parent rows (see orphaned key evidence).",
            "Re-run the DAG after the backfill; the relationships test should pass without code changes.",
        ],
    }.get(
        diagnosis.category,
        ["No safe automated change was found; assign the incident to the on-call data engineer."],
    )
    return _operational(
        f"Operational fix: {diagnosis.category.replace('_', ' ')} on {culprit.describe()}",
        "The root cause is in upstream data, not in this repository, so no code change is proposed.",
        steps,
    )


def _parse_expression(column: str, value_format: str, declared_type: str) -> str | None:
    match value_format:
        case ValueFormat.US_DATE:
            return f"cast(strptime({column}, '%m/%d/%Y') as {declared_type.lower()})"
        case ValueFormat.NUMBER_WITH_SEPARATORS | ValueFormat.CURRENCY:
            return f"cast(regexp_replace({column}, '[^0-9.-]', '', 'g') as {declared_type.lower()})"
        case ValueFormat.ISO_DATE | ValueFormat.INTEGER | ValueFormat.DECIMAL:
            return f"cast({column} as {declared_type.lower()})"
        case _:
            return None


def _assets_sentence(assets: list[str]) -> str:
    dashboards = [asset for asset in assets if " " in asset or asset[:1].isupper()]
    if not dashboards:
        return "Downstream tables keep their last successful data until this is fixed."
    return f"Until it is fixed, {', '.join(dashboards)} will show stale or incomplete numbers."


def _model_name(files: list[str]) -> str:
    sql_files = [path for path in files if path.endswith(".sql")]
    return PurePosixPath(sql_files[0]).stem if sql_files else (files[0] if files else "unknown")


def _missing_column(message: str) -> str | None:
    match = _MISSING_COLUMN.search(message)
    return next((group.lower() for group in match.groups() if group), None) if match else None


def _commit(context: IncidentContext, sha: str | None) -> CommitChange | None:
    return next((commit for commit in context.commits if sha and commit.sha.startswith(sha)), None)


def _drift_for(context: IncidentContext, relation: str) -> SchemaDrift | None:
    return next((drift for drift in context.schema_drift if drift.source == relation), None)


def _metric_count(result: CheckResult, name: str) -> int:
    value = result.metrics.get(name)
    return int(value) if isinstance(value, int | float) else 0


def _of_kind(results: list[CheckResult], kind: CheckKind) -> list[CheckResult]:
    return [result for result in results if result.request.kind is kind and not result.error]


def _evidence_for(investigation: Investigation, hypothesis: Hypothesis) -> list[CheckResult]:
    return [result for result in investigation.evidence if result.request.relation == hypothesis.culprit.relation]


def _failure_message(context: IncidentContext) -> str:
    return context.primary.message if context.primary else context.log_excerpt


def _score_commit(commit: CommitChange) -> _Score:
    score = _Score()
    score.add(0.35, f"commit {commit.sha[:8]} ({commit.message!r}) changed SQL in {commit.lineage_files}")
    return score
