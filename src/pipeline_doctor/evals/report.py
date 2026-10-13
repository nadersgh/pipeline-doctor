import json
import statistics
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel, Field

from pipeline_doctor.incidents.models import FixKind, RootCauseCategory
from pipeline_doctor.jev.models import JevDecision


class CaseResult(BaseModel):
    case_id: str
    title: str
    expected_category: RootCauseCategory
    expected_culprit: str
    has_distractor: bool
    fault_triggered: bool
    predicted_category: RootCauseCategory | None = None
    predicted_culprit: str | None = None
    confidence: float | None = None
    category_correct: bool = False
    culprit_correct: bool = False
    fix_kind: FixKind | None = None
    fix_valid: bool = False
    fix_note: str = ""
    jev_decision: JevDecision | None = None
    jev_score: float | None = None
    latency_seconds: float = 0.0
    cost_usd: float = 0.0
    llm_calls: int = 0
    queries_run: int = 0
    queries_blocked: int = 0
    injection_findings: int = 0
    error: str | None = None

    @property
    def trusted_correctly(self) -> bool:
        return self.category_correct and self.culprit_correct and self.fix_valid


class CategoryRow(BaseModel):
    category: RootCauseCategory
    cases: int
    category_accuracy: float
    culprit_accuracy: float
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


def summarize(
    reasoner: str, started_at: datetime, results: list[CaseResult], *, jev_enabled: bool = True
) -> EvalSummary:
    # Rates use every case: a fault that fails to break the pipeline counts against the run, never silently drops out.
    scored = results
    grouped: dict[RootCauseCategory, list[CaseResult]] = defaultdict(list)
    for result in scored:
        grouped[result.expected_category].append(result)
    latencies = sorted(result.latency_seconds for result in scored) or [0.0]
    distractors = [result for result in scored if result.has_distractor]
    triggered = [result for result in results if result.fault_triggered]
    proceeded = [result for result in triggered if result.jev_decision is JevDecision.PROCEED]
    wrong = [result for result in triggered if not (result.category_correct and result.culprit_correct)]
    return EvalSummary(
        reasoner=reasoner,
        started_at=started_at,
        cases=len(results),
        triggered=len(triggered),
        jev_enabled=jev_enabled,
        category_accuracy=_rate(scored, "category_correct"),
        culprit_accuracy=_rate(scored, "culprit_correct"),
        fix_validity=_rate(scored, "fix_valid"),
        distractor_category_accuracy=_rate(distractors, "category_correct"),
        jev_proceed_rate=round(len(proceeded) / len(triggered), 4) if triggered else 0.0,
        jev_proceed_precision=_rate(proceeded, "trusted_correctly"),
        jev_catch_rate=round(sum(1 for r in wrong if r.jev_decision is not JevDecision.PROCEED) / len(wrong), 4)
        if wrong
        else None,
        mean_latency_seconds=round(statistics.fmean(latencies), 3),
        p95_latency_seconds=round(latencies[min(len(latencies) - 1, int(0.95 * len(latencies)))], 3),
        total_cost_usd=round(sum(result.cost_usd for result in scored), 4),
        mean_queries=round(statistics.fmean([result.queries_run for result in scored] or [0]), 2),
        by_category=[
            CategoryRow(
                category=category,
                cases=len(items),
                category_accuracy=_rate(items, "category_correct"),
                culprit_accuracy=_rate(items, "culprit_correct"),
                fix_validity=_rate(items, "fix_valid"),
            )
            for category, items in sorted(grouped.items())
        ],
    )


def render_markdown(summary: EvalSummary, results: list[CaseResult]) -> str:
    lines = [
        f"# Pipeline Doctor eval: `{summary.reasoner}`",
        "",
        f"Run started {summary.started_at:%Y-%m-%d %H:%M UTC}; {summary.triggered}/{summary.cases} faults triggered a failure.",
        "",
        "| Metric | Value |",
        "| --- | --- |",
        f"| Root-cause category accuracy | {summary.category_accuracy:.1%} |",
        f"| Culprit accuracy | {summary.culprit_accuracy:.1%} |",
        f"| Fix validity (rebuild passes / correct escalation) | {summary.fix_validity:.1%} |",
        f"| Category accuracy on cases with distractor commits | {summary.distractor_category_accuracy:.1%} |",
        *_jev_rows(summary),
        f"| Mean / p95 latency | {summary.mean_latency_seconds:.2f}s / {summary.p95_latency_seconds:.2f}s |",
        f"| Total LLM cost | ${summary.total_cost_usd:.4f} |",
        f"| Mean diagnostic queries per incident | {summary.mean_queries} |",
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
        "| Case | Expected | Predicted | Culprit | Fix | JEV | Latency |",
        "| --- | --- | --- | --- | --- | --- | --- |",
        *[_case_row(result) for result in results],
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
        p95 = ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]
        rows.append(f"| `{name}` | {len(values)} | {statistics.fmean(values):.1f} | {p95:.1f} |")
    header = [
        "## Latency by step (from OpenTelemetry spans)",
        "",
        "| Span | Count | Mean ms | p95 ms |",
        "| --- | --- | --- | --- |",
    ]
    return "\n".join([*header, *rows, ""])


def _case_row(result: CaseResult) -> str:
    if not result.fault_triggered:
        return f"| {result.case_id} | {result.expected_category} | fault did not trigger | | | | |"
    mark = "✅" if result.category_correct else "❌"
    culprit_mark = "✅" if result.culprit_correct else "❌"
    fix_mark = "✅" if result.fix_valid else "❌"
    return (
        f"| {result.case_id} | {result.expected_category} | {mark} {result.predicted_category} "
        f"({result.confidence}) | {culprit_mark} {result.predicted_culprit} | {fix_mark} {result.fix_kind} | "
        f"{result.jev_decision or ''} | {result.latency_seconds:.2f}s |"
    )


def _jev_rows(summary: EvalSummary) -> list[str]:
    if not summary.jev_enabled:
        return ["| JEV decision layer | disabled |"]
    return [
        f"| JEV proceed rate (fix sent to a human as trusted) | {summary.jev_proceed_rate:.1%} |",
        f"| JEV proceed precision (trusted and actually right) | {summary.jev_proceed_precision:.1%} |",
        f"| JEV catch rate (wrong diagnoses held back) | {_catch_rate(summary.jev_catch_rate)} |",
    ]


def _catch_rate(rate: float | None) -> str:
    return "n/a (no wrong diagnoses)" if rate is None else f"{rate:.1%}"


def _rate(results: list[CaseResult], field: str) -> float:
    if not results:
        return 0.0
    return round(sum(1 for result in results if getattr(result, field)) / len(results), 4)
