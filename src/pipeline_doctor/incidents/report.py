from pipeline_doctor.incidents.models import FixKind, Incident
from pipeline_doctor.jev.models import JEV_DECISION_DESCRIPTIONS


def render_incident_report(incident: Incident) -> str:
    """Markdown report shared by the CLI, the API, Slack, and pull request descriptions."""
    lines = [f"# Incident {incident.id}: {incident.event.describe()}", "", f"**Status:** {incident.status}"]
    if incident.error:
        lines += ["", f"**Agent error:** {incident.error}"]
    if incident.injection_findings:
        lines += [
            "",
            "## Untrusted input warnings",
            *[f"- {finding.describe()}" for finding in incident.injection_findings],
        ]
    diagnosis = incident.diagnosis
    if diagnosis is not None:
        lines += [
            "",
            "## What happened (plain language)",
            diagnosis.plain_summary,
            "",
            "## Root cause",
            f"- **Category:** `{diagnosis.category}` (confidence {diagnosis.confidence:.0%})",
            f"- **Culprit:** `{diagnosis.culprit.describe()}`",
            f"- **Impacted assets:** {', '.join(diagnosis.impacted_assets) or 'none recorded'}",
            "",
            diagnosis.technical_summary,
            "",
            "## Evidence",
            *[f"- {item}" for item in diagnosis.evidence],
        ]
    fix = incident.fix
    if fix is not None:
        lines += ["", f"## Proposed fix ({fix.kind}): {fix.title}", fix.rationale]
        if fix.kind is FixKind.OPERATIONAL:
            lines += [f"{index}. {step}" for index, step in enumerate(fix.operational_steps, start=1)]
        if fix.diff:
            lines += ["", "```diff", fix.diff.rstrip(), "```"]
    verdict = incident.verdict
    if verdict is not None:
        lines += [
            "",
            f"## JEV decision: `{verdict.decision}` (score {verdict.score:.0%}, reviewer `{verdict.reviewer}`)",
            JEV_DECISION_DESCRIPTIONS[verdict.decision],
            "",
            *[f"- {check.describe()}" for check in verdict.checks],
        ]
    metrics = incident.metrics
    lines += [
        "",
        "## Run metrics",
        f"reasoner `{incident.reasoner}` · {metrics.latency_seconds:.2f}s · {metrics.queries_run} read-only queries "
        f"({metrics.queries_blocked} blocked) · {metrics.llm_calls} LLM calls · ${metrics.cost_usd:.4f}",
    ]
    if incident.pull_request_url:
        lines += ["", f"**Draft PR:** {incident.pull_request_url}"]
    return "\n".join(lines) + "\n"
