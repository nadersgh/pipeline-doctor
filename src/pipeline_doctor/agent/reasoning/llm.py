from pydantic import BaseModel, Field

from pipeline_doctor.agent.reasoning.base import Assessment, Hypothesis, Investigation
from pipeline_doctor.agent.reasoning.llm_clients import LLMClient
from pipeline_doctor.agent.reasoning.prompts import (
    ASSESS_INSTRUCTIONS,
    FIX_INSTRUCTIONS,
    HYPOTHESIZE_INSTRUCTIONS,
    SYSTEM_PROMPT,
    render_fix_kinds,
)
from pipeline_doctor.diagnostics.checks import CheckRequest
from pipeline_doctor.incidents.models import Diagnosis, FileEdit, FixKind, FixProposal
from pipeline_doctor.security.untrusted import UntrustedSource, fence

_MAX_PRE_CHANGE_CHARS = 12_000


class HypothesisList(BaseModel):
    hypotheses: list[Hypothesis]


class AssessmentDraft(BaseModel):
    ready: bool
    diagnosis: Diagnosis
    follow_up_checks: list[CheckRequest] = Field(default_factory=list)


class FixDraft(BaseModel):
    kind: FixKind
    title: str
    rationale: str
    edits: list[FileEdit] = Field(default_factory=list)
    operational_steps: list[str] = Field(default_factory=list)


class LLMReasoner:
    """Reasoner backed by a language model; every step returns schema-validated structured output."""

    def __init__(self, client: LLMClient, model_label: str, dialect: str) -> None:
        self._client = client
        self._model_label = model_label
        self._dialect = dialect

    @property
    def name(self) -> str:
        return f"llm:{self._model_label}"

    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]:
        prompt = HYPOTHESIZE_INSTRUCTIONS.format(
            round=investigation.rounds + 1,
            context=investigation.context.render(),
            hypotheses=investigation.render_hypotheses(),
            evidence=investigation.render_evidence(),
        )
        if investigation.critique:
            prompt += f"\n\n## Reviewer rejected the previous diagnosis\n{investigation.critique}"
        return self._client.generate(SYSTEM_PROMPT, prompt, HypothesisList).hypotheses

    def assess(self, investigation: Investigation) -> Assessment:
        prompt = ASSESS_INSTRUCTIONS.format(
            context=investigation.context.render(),
            hypotheses=investigation.render_hypotheses(),
            evidence=investigation.render_evidence(),
        )
        draft = self._client.generate(SYSTEM_PROMPT, prompt, AssessmentDraft)
        return Assessment(ready=draft.ready, diagnosis=draft.diagnosis, follow_up_checks=draft.follow_up_checks)

    def propose_fix(self, investigation: Investigation, feedback: str) -> FixProposal:
        context = investigation.context
        diagnosis = investigation.diagnosis.model_dump_json(indent=2) if investigation.diagnosis else "none"
        pre_change = "\n\n".join(
            f"### {key}\n```\n{content}\n```" for key, content in sorted(context.pre_change_files.items())
        )[:_MAX_PRE_CHANGE_CHARS]
        prompt = FIX_INSTRUCTIONS.format(
            diagnosis=fence(diagnosis, UntrustedSource.AGENT_OUTPUT),
            files=context.render_project_files(),
            pre_change=fence(pre_change, UntrustedSource.GIT_HISTORY) if pre_change else "(no suspect commits)",
            feedback=f"## Feedback on your previous attempt\n{feedback}" if feedback else "",
            fix_kinds=render_fix_kinds(),
            dialect=self._dialect,
        )
        draft = self._client.generate(SYSTEM_PROMPT, prompt, FixDraft)
        return FixProposal(**draft.model_dump())
