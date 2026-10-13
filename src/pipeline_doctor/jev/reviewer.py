import re
from pathlib import Path, PurePosixPath
from typing import Protocol

from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.agent.fixes import FixApplicationError, apply_edits, render_diff
from pipeline_doctor.agent.reasoning.base import Investigation, ReasonerError
from pipeline_doctor.incidents.models import Diagnosis, FixKind, FixProposal, RootCauseCategory
from pipeline_doctor.jev.models import JevCheck, JevDecision, JevStage, JevVerdict
from pipeline_doctor.jev.policy import REMEDY_POLICY, is_code_category, is_source_category

_TEST_MARKER = re.compile(
    r"\b(not_null|unique|accepted_values|relationships|unique_combination_of_columns|dbt_utils\.\w+|"
    r"dbt_expectations\.\w+)\b"
)
_TEST_WEAKENER = re.compile(r"(severity:\s*warn|enabled:\s*false|where:\s*)", re.IGNORECASE)
_WRITE_SQL = re.compile(
    r"\b(drop|delete|truncate|insert|merge|grant|revoke|alter|create|update|pre_hook|post_hook|run_query)\b",
    re.IGNORECASE,
)


class JevRequest(BaseModel):
    investigation: Investigation
    fix: FixProposal
    converged: bool = Field(default=True, description="Whether the reasoner judged its own evidence sufficient.")
    can_reinvestigate: bool
    can_revise: bool


class JevReviewer(Protocol):
    @property
    def name(self) -> str: ...

    def review(self, request: JevRequest) -> JevVerdict: ...


def decide(checks: list[JevCheck], request: JevRequest, reviewer: str) -> JevVerdict:
    """Judge failures send the agent back to investigate; evaluate/verify failures send it back to the fix."""
    total = sum(check.weight for check in checks) or 1.0
    score = round(sum(check.weight for check in checks if check.passed) / total, 4)
    judge_failed = any(not check.passed for check in checks if check.stage is JevStage.JUDGE)
    fix_failed = any(not check.passed for check in checks if check.stage is not JevStage.JUDGE)
    if judge_failed:
        decision = JevDecision.REINVESTIGATE if request.can_reinvestigate else JevDecision.ESCALATE
    elif fix_failed:
        decision = JevDecision.REVISE_FIX if request.can_revise else JevDecision.ESCALATE
    else:
        decision = JevDecision.PROCEED
    return JevVerdict(decision=decision, score=score, checks=checks, reviewer=reviewer)


class RuleBasedJev:
    """Deterministic JEV: cheap, offline, and the hard gate the LLM judge can tighten but never loosen."""

    def __init__(self, project_dir: Path, min_confidence: float = 0.6) -> None:
        self._project_dir = project_dir
        self._min_confidence = min_confidence

    @property
    def name(self) -> str:
        return "rules"

    def review(self, request: JevRequest) -> JevVerdict:
        investigation, fix = request.investigation, request.fix
        diagnosis = investigation.diagnosis
        checks = [*self.judge(investigation, diagnosis), *self.evaluate(diagnosis, fix, investigation)]
        checks += self.verify(fix)
        if not request.converged:
            checks.append(
                JevCheck(
                    stage=JevStage.JUDGE,
                    name="investigation_converged",
                    passed=False,
                    weight=2.0,
                    detail="the reasoner did not consider its evidence sufficient",
                )
            )
        return decide(checks, request, self.name)

    def judge(self, investigation: Investigation, diagnosis: Diagnosis | None) -> list[JevCheck]:
        if diagnosis is None:
            return [JevCheck(stage=JevStage.JUDGE, name="diagnosis_present", passed=False, detail="no diagnosis")]
        culprit = diagnosis.culprit.relation
        in_lineage = _matches_any(culprit, _known_relations(investigation))
        return [
            JevCheck(
                stage=JevStage.JUDGE,
                name="category_named",
                passed=diagnosis.category is not RootCauseCategory.UNKNOWN,
                weight=2.0,
                detail=f"category {diagnosis.category}",
            ),
            JevCheck(
                stage=JevStage.JUDGE,
                name="confidence_calibrated",
                passed=diagnosis.confidence >= self._min_confidence,
                detail=f"confidence {diagnosis.confidence:.2f} vs threshold {self._min_confidence:.2f}",
            ),
            JevCheck(
                stage=JevStage.JUDGE,
                name="culprit_in_lineage",
                passed=in_lineage,
                weight=2.0,
                detail=f"{culprit} {'is' if in_lineage else 'is not'} in the failing lineage",
            ),
            _grounding_check(investigation, diagnosis),
        ]

    def evaluate(self, diagnosis: Diagnosis | None, fix: FixProposal, investigation: Investigation) -> list[JevCheck]:
        category = diagnosis.category if diagnosis else RootCauseCategory.UNKNOWN
        allowed = REMEDY_POLICY[category]
        checks = [
            JevCheck(
                stage=JevStage.EVALUATE,
                name="remedy_fits_cause",
                passed=fix.kind in allowed,
                weight=2.0,
                detail=f"{fix.kind} for {category}; allowed {sorted(allowed)}",
            ),
            _actionable_check(fix),
        ]
        if fix.kind is FixKind.OPERATIONAL:
            return checks
        return [*checks, _scope_check(fix, investigation), _tests_preserved_check(fix)]

    def verify(self, fix: FixProposal) -> list[JevCheck]:
        if fix.kind is FixKind.OPERATIONAL:
            return []
        return [self._patch_check(fix), _read_only_check(fix)]

    def _patch_check(self, fix: FixProposal) -> JevCheck:
        try:
            diff = render_diff(self._project_dir, apply_edits(self._project_dir, fix.edits))
        except FixApplicationError as error:
            return JevCheck(stage=JevStage.VERIFY, name="patch_applies", passed=False, weight=2.0, detail=str(error))
        changed = bool(diff.strip())
        return JevCheck(
            stage=JevStage.VERIFY,
            name="patch_applies",
            passed=changed,
            weight=2.0,
            detail="patch applies cleanly" if changed else "patch applies but changes nothing",
        )


class LayeredJev:
    """Runs the rule gate first; a second reviewer (the LLM judge) may veto a PROCEED but cannot overturn a failure."""

    def __init__(self, rules: JevReviewer, judge: JevReviewer) -> None:
        self._rules = rules
        self._judge = judge

    @property
    def name(self) -> str:
        return f"{self._rules.name}+{self._judge.name}"

    def review(self, request: JevRequest) -> JevVerdict:
        verdict = self._rules.review(request)
        if verdict.decision is not JevDecision.PROCEED:
            return verdict.model_copy(update={"reviewer": self.name})
        try:
            second = self._judge.review(request)
        except ReasonerError:
            logger.exception(f"{self._judge.name} JEV failed; keeping the rule verdict")
            return verdict.model_copy(update={"reviewer": self._rules.name})
        return decide([*verdict.checks, *second.checks], request, self.name)


def _grounding_check(investigation: Investigation, diagnosis: Diagnosis) -> JevCheck:
    context = investigation.context
    culprit = diagnosis.culprit
    facts: list[str] = []
    if is_source_category(diagnosis.category):
        facts += [
            f"query {result.request.kind} on {result.request.relation}"
            for result in investigation.evidence
            if result.error is None and _same(result.request.relation, culprit.relation)
        ]
        facts += [
            f"schema drift on {drift.source}"
            for drift in context.schema_drift
            if drift.has_drift and _same(drift.source, culprit.relation)
        ]
        primary = context.primary
        if primary and _matches_any(culprit.relation, [primary.traced_source or "", primary.name]):
            facts.append(f"failing node reads from {culprit.relation}")
    elif is_code_category(diagnosis.category):
        facts += [
            f"commit {commit.sha[:8]} changed lineage SQL"
            for commit in context.commits
            if commit.changes_lineage_sql and (not culprit.commit_sha or commit.sha.startswith(culprit.commit_sha[:7]))
        ]
    grounded = bool(facts) and bool(diagnosis.evidence)
    detail = "; ".join(facts[:3]) if grounded else "no gathered fact supports this culprit"
    if facts and not diagnosis.evidence:
        detail = "diagnosis cites no evidence"
    return JevCheck(stage=JevStage.JUDGE, name="evidence_grounded", passed=grounded, weight=2.0, detail=detail)


def _actionable_check(fix: FixProposal) -> JevCheck:
    if fix.kind is FixKind.OPERATIONAL:
        ok = bool(fix.operational_steps) and not fix.edits
        detail = f"{len(fix.operational_steps)} operational steps" if ok else "operational fix needs steps, no edits"
    else:
        ok = bool(fix.edits)
        detail = f"{len(fix.edits)} edits" if ok else f"{fix.kind} fix has no edits"
    return JevCheck(stage=JevStage.EVALUATE, name="actionable", passed=ok, detail=detail)


def _scope_check(fix: FixProposal, investigation: Investigation) -> JevCheck:
    known = set(investigation.context.project_files) | set(investigation.context.lineage_files)
    known_dirs = {PurePosixPath(path).parent for path in known}
    outside = [
        edit.path
        for edit in fix.edits
        if edit.path not in known
        and PurePosixPath(edit.path).parent not in known_dirs
        and PurePosixPath(edit.path).parts[:1] != ("tests",)
    ]
    return JevCheck(
        stage=JevStage.EVALUATE,
        name="edits_in_lineage",
        passed=not outside,
        detail=f"edits outside the failing lineage: {outside}" if outside else "edits stay in the failing lineage",
    )


def _tests_preserved_check(fix: FixProposal) -> JevCheck:
    weakened = []
    for edit in fix.edits:
        removed = len(_TEST_MARKER.findall(edit.original)) - len(_TEST_MARKER.findall(edit.updated))
        loosened = len(_TEST_WEAKENER.findall(edit.updated)) > len(_TEST_WEAKENER.findall(edit.original))
        deleted_test = PurePosixPath(edit.path).parts[:1] == ("tests",) and edit.original and not edit.updated.strip()
        if removed > 0 or loosened or deleted_test:
            weakened.append(edit.path)
    return JevCheck(
        stage=JevStage.EVALUATE,
        name="tests_preserved",
        passed=not weakened,
        weight=2.0,
        detail=f"fix removes or loosens tests in {weakened}" if weakened else "no test removed or loosened",
    )


def _read_only_check(fix: FixProposal) -> JevCheck:
    offending = [
        edit.path
        for edit in fix.edits
        if edit.path.endswith(".sql") and len(_WRITE_SQL.findall(edit.updated)) > len(_WRITE_SQL.findall(edit.original))
    ]
    return JevCheck(
        stage=JevStage.VERIFY,
        name="no_write_sql",
        passed=not offending,
        weight=2.0,
        detail=f"fix adds DDL/DML or hooks in {offending}" if offending else "no DDL/DML or hooks added",
    )


def _known_relations(investigation: Investigation) -> list[str]:
    context = investigation.context
    relations = [*context.upstream_sources, *context.upstream_models, *(drift.source for drift in context.schema_drift)]
    relations += [result.request.relation for result in investigation.evidence if result.request.relation]
    relations += [PurePosixPath(path).stem for path in context.lineage_files]
    for node in context.failures:
        relations += [node.name, node.tested_relation or "", node.traced_source or "", node.parent_traced_source or ""]
    return [relation for relation in relations if relation]


def _normalize(relation: str) -> str:
    return relation.replace('"', "").strip().lower()


def _same(left: str, right: str) -> bool:
    left, right = _normalize(left), _normalize(right)
    return bool(left and right) and (left == right or left.endswith(f".{right}") or right.endswith(f".{left}"))


def _matches_any(relation: str, candidates: list[str]) -> bool:
    return any(_same(relation, candidate) for candidate in candidates)
