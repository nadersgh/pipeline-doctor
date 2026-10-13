from typing import Protocol

from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.agent.context import IncidentContext
from pipeline_doctor.diagnostics.checks import CheckRequest, CheckResult
from pipeline_doctor.incidents.models import Culprit, Diagnosis, FixProposal, RootCauseCategory


class Hypothesis(BaseModel):
    category: RootCauseCategory
    culprit: Culprit
    rationale: str
    checks: list[CheckRequest] = Field(default_factory=list)


class Investigation(BaseModel):
    """Everything known so far; the single input every reasoning step receives."""

    context: IncidentContext
    hypotheses: list[Hypothesis] = Field(default_factory=list)
    evidence: list[CheckResult] = Field(default_factory=list)
    rounds: int = 0
    diagnosis: Diagnosis | None = None
    critique: str = Field(default="", description="JEV feedback on the previous diagnosis, when it was rejected.")

    def render_evidence(self) -> str:
        if not self.evidence:
            return "- no diagnostic queries run yet"
        return "\n".join(f"- {result.describe()}" for result in self.evidence)

    def render_hypotheses(self) -> str:
        if not self.hypotheses:
            return "- none yet"
        return "\n".join(
            f"- {hypothesis.category} on {hypothesis.culprit.describe()}: {hypothesis.rationale}"
            for hypothesis in self.hypotheses
        )


class Assessment(BaseModel):
    ready: bool = Field(description="True when the evidence supports a conclusion.")
    diagnosis: Diagnosis | None = None
    follow_up_checks: list[CheckRequest] = Field(default_factory=list)


class ReasonerError(RuntimeError):
    pass


class Reasoner(Protocol):
    @property
    def name(self) -> str: ...

    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]: ...

    def assess(self, investigation: Investigation) -> Assessment: ...

    def propose_fix(self, investigation: Investigation, feedback: str) -> FixProposal: ...


class FallbackReasoner:
    """Uses the primary reasoner and degrades to the fallback for any step where the primary fails."""

    def __init__(self, primary: Reasoner, fallback: Reasoner) -> None:
        self._primary = primary
        self._fallback = fallback

    @property
    def name(self) -> str:
        return f"{self._primary.name}+{self._fallback.name}"

    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]:
        try:
            return self._primary.hypothesize(investigation)
        except ReasonerError:
            logger.exception(f"{self._primary.name} failed to hypothesize; using {self._fallback.name}")
            return self._fallback.hypothesize(investigation)

    def assess(self, investigation: Investigation) -> Assessment:
        try:
            return self._primary.assess(investigation)
        except ReasonerError:
            logger.exception(f"{self._primary.name} failed to assess; using {self._fallback.name}")
            return self._fallback.assess(investigation)

    def propose_fix(self, investigation: Investigation, feedback: str) -> FixProposal:
        try:
            return self._primary.propose_fix(investigation, feedback)
        except ReasonerError:
            logger.exception(f"{self._primary.name} failed to propose a fix; using {self._fallback.name}")
            return self._fallback.propose_fix(investigation, feedback)
