import time
from typing import Literal

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph
from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.agent.context import IncidentContext
from pipeline_doctor.agent.fixes import FixApplicationError, apply_edits, render_diff
from pipeline_doctor.agent.reasoning.base import Assessment, Hypothesis, Investigation, Reasoner
from pipeline_doctor.agent.toolkit import ContextGatherer, DiagnosticToolkit
from pipeline_doctor.diagnostics.checks import CheckRequest, CheckResult
from pipeline_doctor.incidents.models import (
    Culprit,
    Diagnosis,
    FailureEvent,
    FixKind,
    FixProposal,
    Incident,
    IncidentStatus,
    RootCauseCategory,
    RunMetrics,
)
from pipeline_doctor.jev.models import JevDecision, JevVerdict
from pipeline_doctor.jev.reviewer import JevRequest, JevReviewer
from pipeline_doctor.observability.tracing import traced
from pipeline_doctor.observability.usage import UsageMeter
from pipeline_doctor.security.untrusted import UntrustedSource, scan_for_injection

_MAX_FIX_ATTEMPTS = 2


class AgentState(BaseModel):
    event: FailureEvent
    context: IncidentContext | None = None
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    evidence: list[CheckResult] = Field(default_factory=list)
    pending_checks: list[CheckRequest] = Field(default_factory=list)
    assessment: Assessment | None = None
    rounds: int = 0
    diagnosis: Diagnosis | None = None
    fix: FixProposal | None = None
    verdict: JevVerdict | None = None
    critique: str = ""
    fix_feedback: str = ""
    fix_revisions: int = 0
    notes: list[str] = Field(default_factory=list)

    def investigation(self) -> Investigation:
        if self.context is None:
            raise RuntimeError("context must be gathered before reasoning")
        return Investigation(
            context=self.context,
            hypotheses=self.hypotheses,
            evidence=self.evidence,
            rounds=self.rounds,
            diagnosis=self.diagnosis,
            critique=self.critique,
        )


def _manual_review_fix(diagnosis: Diagnosis | None) -> FixProposal:
    leading = f"{diagnosis.category} on {diagnosis.culprit.describe()}" if diagnosis else "no hypothesis"
    return FixProposal(
        kind=FixKind.OPERATIONAL,
        title="Low-confidence diagnosis: manual review required",
        rationale=f"The investigation ran out of rounds without a confident conclusion (leading hypothesis: {leading}). "
        "No code change is proposed from an unconfirmed diagnosis.",
        operational_steps=["Review the evidence in this report.", "Assign the incident to the on-call data engineer."],
    )


def _escalation_fix(fix: FixProposal, verdict: JevVerdict) -> FixProposal:
    return FixProposal(
        kind=FixKind.OPERATIONAL,
        title=f"Escalated by JEV: {fix.title}",
        rationale=f"JEV did not trust the diagnosis or fix enough to propose a code change. {verdict.feedback()}",
        operational_steps=[
            "Review the JEV findings and the evidence in this report.",
            "Assign the incident to the on-call data engineer.",
        ],
    )


class PipelineDoctor:
    """Read-only diagnosis agent. It never writes: fixes are proposals that wait for human approval."""

    def __init__(
        self,
        toolkit: DiagnosticToolkit,
        reasoner: Reasoner,
        meter: UsageMeter,
        jev: JevReviewer | None = None,
        max_rounds: int = 2,
        max_fix_revisions: int = 1,
    ) -> None:
        self._toolkit = toolkit
        self._reasoner = reasoner
        self._meter = meter
        self._jev = jev
        self._max_rounds = max_rounds
        self._max_fix_revisions = max_fix_revisions
        self._graph = self._build_graph()

    def diagnose(self, event: FailureEvent) -> Incident:
        incident = Incident(event=event, reasoner=self._reasoner.name)
        started = time.perf_counter()
        with traced("pipeline_doctor.diagnose", {"incident.id": incident.id, "reasoner": self._reasoner.name}) as span:
            final = AgentState(event=event)
            try:
                for snapshot in self._graph.stream(final, stream_mode="values"):
                    final = AgentState.model_validate(snapshot)
            except Exception as error:
                logger.exception(f"diagnosis of {event.describe()} failed")
                return incident.model_copy(
                    update={
                        "status": IncidentStatus.FAILED,
                        "error": f"{type(error).__name__}: {error}",
                        "diagnosis": final.diagnosis,
                        "metrics": self._metrics(started, final.rounds),
                        "signature": final.context.signature() if final.context else "",
                        "injection_findings": final.context.injection_findings if final.context else [],
                    }
                )
            metrics = self._metrics(started, final.rounds)
            span.set_attribute("diagnosis.category", final.diagnosis.category if final.diagnosis else "none")
            span.set_attribute("jev.decision", final.verdict.decision if final.verdict else "none")
            span.set_attribute("cost.usd", metrics.cost_usd)
            span.set_attribute("queries.run", metrics.queries_run)
        return incident.model_copy(
            update={
                "status": IncidentStatus.AWAITING_APPROVAL,
                "diagnosis": final.diagnosis,
                "fix": final.fix,
                "verdict": final.verdict,
                "metrics": metrics,
                "signature": final.context.signature() if final.context else "",
                "injection_findings": final.context.injection_findings if final.context else [],
            }
        )

    def _build_graph(self) -> CompiledStateGraph[AgentState, None, AgentState, AgentState]:
        graph = StateGraph(AgentState)
        graph.add_node("detect", self._detect)
        graph.add_node("gather_context", self._gather_context)
        graph.add_node("hypothesize", self._hypothesize)
        graph.add_node("verify", self._verify)
        graph.add_node("assess", self._assess)
        graph.add_node("explain", self._explain)
        graph.add_node("propose_fix", self._propose_fix)
        if self._jev is not None:
            graph.add_node("jev", self._review)
        graph.add_node("request_approval", self._request_approval)
        graph.add_edge(START, "detect")
        graph.add_edge("detect", "gather_context")
        graph.add_edge("gather_context", "hypothesize")
        graph.add_edge("hypothesize", "verify")
        graph.add_edge("verify", "assess")
        graph.add_conditional_edges("assess", self._route_after_assessment, ["hypothesize", "explain"])
        graph.add_edge("explain", "propose_fix")
        if self._jev is None:
            graph.add_edge("propose_fix", "request_approval")
        else:
            graph.add_edge("propose_fix", "jev")
            graph.add_conditional_edges(
                "jev", self._route_after_review, ["request_approval", "propose_fix", "hypothesize"]
            )
        graph.add_edge("request_approval", END)
        return graph.compile()  # type: ignore[return-value] # langgraph generics infer StateT, not AgentState

    def _detect(self, state: AgentState) -> dict[str, object]:
        with traced("agent.detect", {"event.source": state.event.source}):
            logger.info(f"detected failure: {state.event.describe()}")
            return {}

    def _gather_context(self, state: AgentState) -> dict[str, object]:
        with traced("agent.gather_context") as span:
            context = ContextGatherer(self._toolkit).gather(state.event)
            span.set_attribute("context.failures", len(context.failures))
            span.set_attribute("context.commits", len(context.commits))
            span.set_attribute("context.schema_drift", len(context.schema_drift))
            return {"context": context}

    def _hypothesize(self, state: AgentState) -> dict[str, object]:
        with traced("agent.hypothesize", {"round": state.rounds}) as span:
            new = self._reasoner.hypothesize(state.investigation())
            follow_ups = state.assessment.follow_up_checks if state.assessment else []
            span.set_attribute("hypotheses.new", len(new))
            checks = [check for hypothesis in new for check in hypothesis.checks] + follow_ups
            return {"hypotheses": [*state.hypotheses, *new], "pending_checks": checks}

    def _verify(self, state: AgentState) -> dict[str, object]:
        with traced("agent.verify") as span:
            seen = {result.request.key() for result in state.evidence}
            results = []
            for request in state.pending_checks:
                if request.key() in seen:
                    continue
                seen.add(request.key())
                results.append(self._toolkit.run_check(request))
            span.set_attribute("checks.run", len(results))
            update: dict[str, object] = {"evidence": [*state.evidence, *results], "pending_checks": []}
            # Errors are skipped: a blocked query's error echoes the model's own SQL, not source data.
            findings = [
                finding
                for result in results
                if result.error is None
                and (
                    finding := scan_for_injection(
                        result.render_output(),
                        UntrustedSource.QUERY_RESULT,
                        f"{result.request.kind} on {result.request.relation}",
                    )
                )
            ]
            if findings and state.context is not None:
                known = state.context.injection_findings
                update["context"] = state.context.model_copy(update={"injection_findings": [*known, *findings]})
            return update

    def _assess(self, state: AgentState) -> dict[str, object]:
        with traced("agent.assess", {"round": state.rounds}) as span:
            assessment = self._reasoner.assess(state.investigation())
            span.set_attribute("assessment.ready", assessment.ready)
            return {"assessment": assessment, "diagnosis": assessment.diagnosis, "rounds": state.rounds + 1}

    def _route_after_assessment(self, state: AgentState) -> Literal["hypothesize", "explain"]:
        ready = state.assessment is not None and state.assessment.ready
        return "explain" if ready or state.rounds >= self._max_rounds else "hypothesize"

    def _explain(self, state: AgentState) -> dict[str, object]:
        with traced("agent.explain"):
            context = state.context
            diagnosis = state.diagnosis or Diagnosis(
                category=RootCauseCategory.UNKNOWN,
                confidence=0.0,
                culprit=Culprit(relation="unknown"),
                technical_summary="The investigation did not converge on a root cause.",
                plain_summary="We could not determine the cause automatically; an engineer has been asked to look.",
            )
            impacted = context.downstream_assets if context else []
            return {"diagnosis": diagnosis.model_copy(update={"impacted_assets": impacted})}

    def _propose_fix(self, state: AgentState) -> dict[str, object]:
        with traced("agent.propose_fix") as span:
            if state.assessment is None or not state.assessment.ready:
                span.set_attribute("fix.kind", FixKind.OPERATIONAL)
                return {"fix": _manual_review_fix(state.diagnosis)}
            investigation = state.investigation()
            feedback = state.fix_feedback
            for attempt in range(1, _MAX_FIX_ATTEMPTS + 1):
                fix = self._reasoner.propose_fix(investigation, feedback)
                try:
                    diff = render_diff(self._toolkit.project_dir, apply_edits(self._toolkit.project_dir, fix.edits))
                except FixApplicationError as error:
                    feedback = (
                        f"Attempt {attempt} could not be applied: {error}. Copy `original` verbatim from the file."
                    )
                    logger.warning(feedback)
                    continue
                span.set_attribute("fix.kind", fix.kind)
                span.set_attribute("fix.attempts", attempt)
                return {"fix": fix.model_copy(update={"diff": diff})}
            fallback = FixProposal(
                kind=FixKind.OPERATIONAL,
                title="Manual fix required",
                rationale=f"The proposed patch could not be applied safely. {feedback}",
                operational_steps=["Review the diagnosis and apply the change by hand."],
            )
            return {"fix": fallback, "notes": [*state.notes, feedback]}

    def _review(self, state: AgentState) -> dict[str, object]:
        if state.fix is None or self._jev is None:
            raise RuntimeError("JEV review needs a reviewer and a proposed fix")
        with traced("agent.jev", {"reviewer": self._jev.name}) as span:
            request = JevRequest(
                investigation=state.investigation(),
                fix=state.fix,
                converged=state.assessment is not None and state.assessment.ready,
                can_reinvestigate=state.rounds < self._max_rounds,
                can_revise=state.fix_revisions < self._max_fix_revisions,
            )
            reviews = state.verdict.reviews + 1 if state.verdict else 1
            verdict = self._jev.review(request).model_copy(update={"reviews": reviews})
            span.set_attribute("jev.decision", verdict.decision)
            span.set_attribute("jev.score", verdict.score)
            span.set_attribute("jev.failed_checks", ",".join(check.name for check in verdict.failed))
            logger.info(f"JEV review {reviews}: {verdict.describe()}")
            update: dict[str, object] = {"verdict": verdict}
            if verdict.decision is JevDecision.REVISE_FIX:
                update |= {"fix_feedback": verdict.feedback(), "fix_revisions": state.fix_revisions + 1}
            elif verdict.decision is JevDecision.REINVESTIGATE:
                update |= {"critique": verdict.feedback(), "fix_feedback": ""}
            elif verdict.decision is JevDecision.ESCALATE and state.fix.kind is not FixKind.OPERATIONAL:
                update |= {"fix": _escalation_fix(state.fix, verdict)}
            return update

    def _route_after_review(self, state: AgentState) -> Literal["request_approval", "propose_fix", "hypothesize"]:
        decision = state.verdict.decision if state.verdict else JevDecision.ESCALATE
        if decision is JevDecision.REVISE_FIX:
            return "propose_fix"
        if decision is JevDecision.REINVESTIGATE:
            return "hypothesize"
        return "request_approval"

    def _request_approval(self, state: AgentState) -> dict[str, object]:
        with traced("agent.request_approval"):
            logger.info(f"diagnosis ready for human review: {state.diagnosis.category if state.diagnosis else 'none'}")
            return {}

    def _metrics(self, started: float, rounds: int) -> RunMetrics:
        budget = self._toolkit.guarded_warehouse.budget
        metrics = RunMetrics(
            latency_seconds=round(time.perf_counter() - started, 3),
            queries_run=budget.queries_run,
            queries_blocked=budget.queries_blocked,
            investigation_rounds=rounds,
        )
        return self._meter.fill(metrics)
