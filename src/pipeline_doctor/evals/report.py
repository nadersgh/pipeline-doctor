import json
import statistics
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

from pipeline_doctor.evals.cases import EvalCaseKind, EvalSplit
from pipeline_doctor.evals.catalogue import SplitFilter
from pipeline_doctor.incidents.models import FixKind, RootCauseCategory
from pipeline_doctor.jev.models import JevDecision


class CaseResult(BaseModel):
    case_id: str
    title: str
    expected_category: RootCauseCategory
    expected_culprit: str
    has_distractor: bool
    fault_triggered: bool
    split: EvalSplit = EvalSplit.DEV
    kind: EvalCaseKind = EvalCaseKind.SINGLE
    repeat: int = 0
    predicted_category: RootCauseCategory | None = None
    predicted_culprit: str | None = None
    confidence: float | None = None
    matched_cause: str | None = Field(default=None, description="Compound cases: the component the diagnosis matched.")
    category_correct: bool = False
    relation_correct: bool = False
    column_correct: bool | None = Field(default=None, description="None when the ground truth names no column.")
    culprit_correct: bool = False
    fix_kind: FixKind | None = None
    fix_kind_correct: bool = False
    fix_valid: bool = False
    fix_note: str = ""
    primary_hit: bool = False
    false_positive: bool | None = Field(default=None, description="Set only for no-fault cases.")
    jev_decision: JevDecision | None = None
    jev_score: float | None = None
    latency_seconds: float = 0.0
    cost_usd: float = 0.0
    llm_calls: int = 0
    queries_run: int = 0
    queries_blocked: int = 0
    error: str | None = None

    @property
    def trusted_correctly(self) -> bool:
        return self.category_correct and self.culprit_correct and self.fix_valid

    @property
    def outcome(self) -> tuple[object, ...]:
        # Scored flags rather than the culprit text: injected commits get a new SHA on every repeat.
        return (
            self.fault_triggered,
            self.predicted_category,
            self.relation_correct,
            self.column_correct,
            self.fix_kind,
            self.fix_valid,
        )


class CategoryRow(BaseModel):
    category: RootCauseCategory
    cases: int
    category_accuracy: float
    culprit_accuracy: float
    fix_validity: float


class ScoreCard(BaseModel):
    cases: int
    runs: int
    category_accuracy: float
    relation_accuracy: float
    column_accuracy: float | None = Field(default=None, description="None when no case names a culprit column.")
    culprit_accuracy: float
    fix_kind_accuracy: float
    fix_validity: float
    primary_hit_rate: float
    false_positive_rate: float | None = Field(default=None, description="None when no no-fault case ran.")
    total_cost_usd: float
    total_latency_seconds: float
    p50_latency_seconds: float
    p95_latency_seconds: float
    llm_calls: int
    queries_run: int


class MetricSpread(BaseModel):
    metric: str
    mean: float
    std: float
    min: float
    max: float


class CaseConsistency(BaseModel):
    case_id: str
    split: EvalSplit
    kind: EvalCaseKind
    runs: int
    agreement: float = Field(description="Share of repeats with the modal outcome (category, culprit hit, fix).")
    primary_hit_rate: float
    fix_validity: float


class EvalSummary(BaseModel):
    reasoner: str
    started_at: datetime
    cases: int
    triggered: int
    category_accuracy: float
    culprit_accuracy: float
    fix_validity: float
    distractor_category_accuracy: float
    jev_enabled: bool = True
    jev_proceed_rate: float = 0.0
    jev_proceed_precision: float = 0.0
    jev_catch_rate: float | None = Field(default=None, description="None when no diagnosis was wrong.")
    mean_latency_seconds: float
    p95_latency_seconds: float
    total_cost_usd: float
    mean_queries: float
    by_category: list[CategoryRow]
    overall: ScoreCard
    split: SplitFilter = SplitFilter.ALL
    repeats: int = 1
    runs: int = 0
    by_split: dict[EvalSplit, ScoreCard] = Field(default_factory=dict)
    by_kind: dict[EvalCaseKind, ScoreCard] = Field(default_factory=dict)
    spread: list[MetricSpread] = Field(default_factory=list, description="Each metric across repeats.")
    consistency: list[CaseConsistency] = Field(default_factory=list)
    mean_agreement: float = 1.0


_SPREAD_METRICS = [
    "category_accuracy",
    "relation_accuracy",
    "column_accuracy",
    "culprit_accuracy",
    "fix_kind_accuracy",
    "fix_validity",
    "primary_hit_rate",
    "false_positive_rate",
    "total_cost_usd",
    "p50_latency_seconds",
    "p95_latency_seconds",
]


def summarize(
    reasoner: str,
    started_at: datetime,
    results: list[CaseResult],
    *,
    jev_enabled: bool = True,
    split: SplitFilter = SplitFilter.ALL,
) -> EvalSummary:
    # Rates use every run: a fault that fails to break the pipeline counts against the run, never silently drops out.
    # With repeats, pooled rates equal the mean of per-repeat rates because every repeat runs the same cases.
    grouped: dict[RootCauseCategory, list[CaseResult]] = defaultdict(list)
    for result in results:
        grouped[result.expected_category].append(result)
    distractors = [result for result in results if result.has_distractor]
    triggered = [result for result in results if result.fault_triggered]
    proceeded = [result for result in triggered if result.jev_decision is JevDecision.PROCEED]
    wrong = [result for result in triggered if not (result.category_correct and result.culprit_correct)]
    overall = score_card(results)
    consistency = measure_consistency(results)
    return EvalSummary(
        reasoner=reasoner,
        started_at=started_at,
        cases=overall.cases,
        triggered=len(triggered),
        jev_enabled=jev_enabled,
        category_accuracy=overall.category_accuracy,
        culprit_accuracy=overall.culprit_accuracy,
        fix_validity=overall.fix_validity,
        distractor_category_accuracy=_rate(distractors, "category_correct"),
        jev_proceed_rate=round(len(proceeded) / len(triggered), 4) if triggered else 0.0,
        jev_proceed_precision=_rate(proceeded, "trusted_correctly"),
        jev_catch_rate=round(sum(1 for r in wrong if r.jev_decision is not JevDecision.PROCEED) / len(wrong), 4)
        if wrong
        else None,
        mean_latency_seconds=round(statistics.fmean([r.latency_seconds for r in results] or [0.0]), 3),
        p95_latency_seconds=overall.p95_latency_seconds,
        total_cost_usd=overall.total_cost_usd,
        mean_queries=round(statistics.fmean([result.queries_run for result in results] or [0]), 2),
        by_category=[
            CategoryRow(
                category=category,
                cases=len({item.case_id for item in items}),
                category_accuracy=_rate(items, "category_correct"),
                culprit_accuracy=_rate(items, "culprit_correct"),
                fix_validity=_rate(items, "fix_valid"),
            )
            for category, items in sorted(grouped.items())
        ],
        split=split,
        repeats=len({result.repeat for result in results}) or 1,
        runs=len(results),
        overall=overall,
        by_split={split: score_card(items) for split, items in _group_by_split(results).items()},
        by_kind={kind: score_card(items) for kind, items in _group_by_kind(results).items()},
        spread=measure_spread([score_card(items) for _, items in sorted(_group_by_repeat(results).items())]),
        consistency=consistency,
        mean_agreement=round(statistics.fmean([row.agreement for row in consistency] or [1.0]), 4),
    )


def score_card(results: list[CaseResult]) -> ScoreCard:
    latencies = sorted(result.latency_seconds for result in results) or [0.0]
    with_column = [result for result in results if result.column_correct is not None]
    no_fault = [result for result in results if result.false_positive is not None]
    return ScoreCard(
        cases=len({result.case_id for result in results}),
        runs=len(results),
        category_accuracy=_rate(results, "category_correct"),
        relation_accuracy=_rate(results, "relation_correct"),
        column_accuracy=_rate(with_column, "column_correct") if with_column else None,
        culprit_accuracy=_rate(results, "culprit_correct"),
        fix_kind_accuracy=_rate(results, "fix_kind_correct"),
        fix_validity=_rate(results, "fix_valid"),
        primary_hit_rate=_rate(results, "primary_hit"),
        false_positive_rate=_rate(no_fault, "false_positive") if no_fault else None,
        total_cost_usd=round(sum(result.cost_usd for result in results), 4),
        total_latency_seconds=round(sum(latencies), 3),
        p50_latency_seconds=round(_percentile(latencies, 0.5), 3),
        p95_latency_seconds=round(_percentile(latencies, 0.95), 3),
        llm_calls=sum(result.llm_calls for result in results),
        queries_run=sum(result.queries_run for result in results),
    )


def measure_spread(cards: list[ScoreCard]) -> list[MetricSpread]:
    """Mean, sample standard deviation, min and max of each metric across per-repeat score cards."""
    spreads = []
    for metric in _SPREAD_METRICS:
        values = [value for card in cards if (value := getattr(card, metric)) is not None]
        if not values:
            continue
        spreads.append(
            MetricSpread(
                metric=metric,
                mean=round(statistics.fmean(values), 4),
                std=round(statistics.stdev(values), 4) if len(values) > 1 else 0.0,
                min=min(values),
                max=max(values),
            )
        )
    return spreads


def measure_consistency(results: list[CaseResult]) -> list[CaseConsistency]:
    rows = []
    for runs in _group_by_case(results).values():
        modal_count = Counter(run.outcome for run in runs).most_common(1)[0][1]
        rows.append(
            CaseConsistency(
                case_id=runs[0].case_id,
                split=runs[0].split,
                kind=runs[0].kind,
                runs=len(runs),
                agreement=round(modal_count / len(runs), 4),
                primary_hit_rate=_rate(runs, "primary_hit"),
                fix_validity=_rate(runs, "fix_valid"),
            )
        )
    return rows


def render_markdown(summary: EvalSummary, results: list[CaseResult]) -> str:
    overall = summary.overall
    lines = [
        f"# Pipeline Doctor eval: `{summary.reasoner}`",
        "",
        f"Run started {summary.started_at:%Y-%m-%d %H:%M UTC}; split `{summary.split}`, {summary.cases} cases x "
        f"{summary.repeats} repeats; {summary.triggered}/{len(results)} runs triggered a failure.",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Root-cause category accuracy | {summary.category_accuracy:.1%} |",
        f"| Affected relation accuracy | {overall.relation_accuracy:.1%} |",
        f"| Culprit column accuracy (cases with a column) | {_percent(overall.column_accuracy)} |",
        f"| Culprit accuracy (relation and column) | {summary.culprit_accuracy:.1%} |",
        f"| Fix kind acceptable | {overall.fix_kind_accuracy:.1%} |",
        f"| Fix validity (rebuild passes / correct escalation) | {summary.fix_validity:.1%} |",
        f"| Primary hit (category and relation match any acceptable cause) | {overall.primary_hit_rate:.1%} |",
        f"| False-positive rate (no-fault cases) | {_percent(overall.false_positive_rate)} |",
        f"| Category accuracy on cases with distractor commits | {_distractor_accuracy(summary, results)} |",
        *_jev_rows(summary),
        f"| Mean / p50 / p95 latency | {summary.mean_latency_seconds:.2f}s / {overall.p50_latency_seconds:.2f}s / "
        f"{summary.p95_latency_seconds:.2f}s |",
        f"| Total LLM cost / calls | ${summary.total_cost_usd:.4f} / {overall.llm_calls} |",
        f"| Mean diagnostic queries per incident | {summary.mean_queries} |",
        f"| Mean per-case agreement across repeats | {summary.mean_agreement:.1%} |",
        "",
        "## By split",
        "",
        *_score_table("Split", summary.by_split),
        "## By case kind",
        "",
        *_score_table("Kind", summary.by_kind),
        "## Variance across repeats",
        "",
        "| Metric | Mean | Std | Min | Max |",
        "| --- | --- | --- | --- | --- |",
        *[f"| {s.metric} | {s.mean:.4g} | {s.std:.4g} | {s.min:.4g} | {s.max:.4g} |" for s in summary.spread],
        "",
        "## By root-cause category",
        "",
        "| Category | Cases | Category acc. | Culprit acc. | Fix validity |",
        "| --- | --- | --- | --- | --- |",
        *[
            f"| {row.category} | {row.cases} | {row.category_accuracy:.0%} | {row.culprit_accuracy:.0%} | {row.fix_validity:.0%} |"
            for row in summary.by_category
        ],
        "",
        "## Cases",
        "",
        "| Case | Split | Kind | Rep | Expected | Predicted (confidence) on culprit | Cat | Rel | Col | Fix kind | Fix valid "
        "| Primary | FP | JEV | Cost | Latency | LLM calls | Queries |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        *[_case_row(result) for result in results],
        "",
        "## Consistency across repeats",
        "",
        "| Case | Runs | Agreement | Primary hit | Fix validity |",
        "| --- | --- | --- | --- | --- |",
        *[
            f"| {row.case_id} | {row.runs} | {row.agreement:.0%} | {row.primary_hit_rate:.0%} | {row.fix_validity:.0%} |"
            for row in summary.consistency
        ],
        "",
    ]
    return "\n".join(lines)


def render_step_latency(spans_file: Path) -> str:
    """Markdown table of mean/p95 duration per traced agent step and tool, from the run's span log."""
    if not spans_file.exists():
        return ""
    durations: dict[str, list[float]] = defaultdict(list)
    for line in spans_file.read_text().splitlines():
        span = json.loads(line)
        durations[span["name"]].append(float(span["duration_ms"]))
    rows = []
    for name, values in sorted(durations.items()):
        ordered = sorted(values)
        rows.append(f"| `{name}` | {len(values)} | {statistics.fmean(values):.1f} | {_percentile(ordered, 0.95):.1f} |")
    header = [
        "## Latency by step (from OpenTelemetry spans)",
        "",
        "| Span | Count | Mean ms | p95 ms |",
        "| --- | --- | --- | --- |",
    ]
    return "\n".join([*header, *rows, ""])


def _score_table(label: str, cards: dict[EvalSplit, ScoreCard] | dict[EvalCaseKind, ScoreCard]) -> list[str]:
    header = [
        f"| {label} | Cases | Runs | Category | Relation | Column | Culprit | Fix kind | Fix valid | Primary hit "
        "| FP rate | Cost | LLM calls | p50 / p95 latency |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    rows = [
        f"| {key} | {card.cases} | {card.runs} | {card.category_accuracy:.1%} | {card.relation_accuracy:.1%} | "
        f"{_percent(card.column_accuracy)} | {card.culprit_accuracy:.1%} | {card.fix_kind_accuracy:.1%} | "
        f"{card.fix_validity:.1%} | {card.primary_hit_rate:.1%} | {_percent(card.false_positive_rate)} | "
        f"${card.total_cost_usd:.4f} | {card.llm_calls} | {card.p50_latency_seconds:.2f}s / {card.p95_latency_seconds:.2f}s |"
        for key, card in cards.items()
    ]
    return [*header, *rows, ""]


def _case_row(result: CaseResult) -> str:
    matched = f" (matched {result.matched_cause})" if result.matched_cause else ""
    prefix = (
        f"| {result.case_id} | {result.split} | {result.kind} | {result.repeat} | {result.expected_category}{matched} |"
    )
    if not result.fault_triggered:
        return f"{prefix} fault did not trigger | | | | | | | | | | | | |"
    culprit = f" on {result.predicted_culprit}" if result.predicted_culprit else ""
    return (
        f"{prefix} {result.predicted_category} ({result.confidence}){culprit} | {_mark(result.category_correct)} | "
        f"{_mark(result.relation_correct)} | {_mark(result.column_correct)} | {_mark(result.fix_kind_correct)} "
        f"{result.fix_kind or ''} | {_mark(result.fix_valid)} | {_mark(result.primary_hit)} | "
        f"{_flag(result.false_positive)} | {result.jev_decision or ''} | ${result.cost_usd:.4f} | "
        f"{result.latency_seconds:.2f}s | {result.llm_calls} | {result.queries_run} |"
    )


def _mark(value: bool | None) -> str:
    if value is None:
        return "n/a"
    return "✅" if value else "❌"


def _flag(value: bool | None) -> str:
    if value is None:
        return ""
    return "⚠️ yes" if value else "no"


def _jev_rows(summary: EvalSummary) -> list[str]:
    if not summary.jev_enabled:
        return ["| JEV decision layer | disabled |"]
    return [
        f"| JEV proceed rate (fix sent to a human as trusted) | {summary.jev_proceed_rate:.1%} |",
        f"| JEV proceed precision (trusted and actually right) | {summary.jev_proceed_precision:.1%} |",
        f"| JEV catch rate (wrong diagnoses held back) | {_catch_rate(summary.jev_catch_rate)} |",
    ]


def _distractor_accuracy(summary: EvalSummary, results: list[CaseResult]) -> str:
    if not any(result.has_distractor for result in results):
        return "n/a (no distractor cases)"
    return f"{summary.distractor_category_accuracy:.1%}"


def _catch_rate(rate: float | None) -> str:
    return "n/a (no wrong diagnoses)" if rate is None else f"{rate:.1%}"


def _percent(rate: float | None) -> str:
    return "n/a" if rate is None else f"{rate:.1%}"


def _percentile(ordered: list[float], quantile: float) -> float:
    return ordered[min(len(ordered) - 1, int(quantile * len(ordered)))]


def _group_by_split(results: list[CaseResult]) -> dict[EvalSplit, list[CaseResult]]:
    grouped = {split: [result for result in results if result.split is split] for split in EvalSplit}
    return {split: items for split, items in grouped.items() if items}


def _group_by_kind(results: list[CaseResult]) -> dict[EvalCaseKind, list[CaseResult]]:
    grouped = {kind: [result for result in results if result.kind is kind] for kind in EvalCaseKind}
    return {kind: items for kind, items in grouped.items() if items}


def _group_by_repeat(results: list[CaseResult]) -> dict[int, list[CaseResult]]:
    grouped: dict[int, list[CaseResult]] = defaultdict(list)
    for result in results:
        grouped[result.repeat].append(result)
    return dict(grouped)


def _group_by_case(results: list[CaseResult]) -> dict[str, list[CaseResult]]:
    grouped: dict[str, list[CaseResult]] = defaultdict(list)
    for result in results:
        grouped[result.case_id].append(result)
    return dict(grouped)


def _rate(results: list[CaseResult], field: str) -> float:
    if not results:
        return 0.0
    return round(sum(1 for result in results if getattr(result, field)) / len(results), 4)
