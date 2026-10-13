from enum import StrEnum

from pydantic import BaseModel, Field


class JevStage(StrEnum):
    JUDGE = "judge"
    EVALUATE = "evaluate"
    VERIFY = "verify"


class JevDecision(StrEnum):
    PROCEED = "proceed"
    REVISE_FIX = "revise_fix"
    REINVESTIGATE = "reinvestigate"
    ESCALATE = "escalate"


JEV_DECISION_DESCRIPTIONS: dict[JevDecision, str] = {
    JevDecision.PROCEED: "Diagnosis and fix hold up; a human may approve the draft PR.",
    JevDecision.REVISE_FIX: "Diagnosis holds up but the fix does not; propose a new fix.",
    JevDecision.REINVESTIGATE: "Diagnosis is not supported by the evidence; run another investigation round.",
    JevDecision.ESCALATE: "Budget exhausted without a trustworthy result; hand to an engineer, no code change.",
}


class JevCheck(BaseModel):
    stage: JevStage
    name: str
    passed: bool
    weight: float = Field(default=1.0, gt=0.0)
    detail: str = ""

    def describe(self) -> str:
        mark = "pass" if self.passed else "FAIL"
        return f"[{self.stage}/{self.name}] {mark}: {self.detail}"


class JevVerdict(BaseModel):
    decision: JevDecision
    score: float = Field(ge=0.0, le=1.0, description="Weighted share of passed checks.")
    checks: list[JevCheck] = Field(default_factory=list)
    reviewer: str = ""
    reviews: int = Field(default=1, description="How many JEV reviews this incident went through.")

    @property
    def failed(self) -> list[JevCheck]:
        return [check for check in self.checks if not check.passed]

    def failed_in(self, *stages: JevStage) -> list[JevCheck]:
        return [check for check in self.failed if check.stage in stages]

    def feedback(self) -> str:
        if not self.failed:
            return ""
        return "JEV review failed:\n" + "\n".join(f"- {check.describe()}" for check in self.failed)

    def describe(self) -> str:
        return f"{self.decision} (score {self.score:.2f}, {len(self.failed)} failed of {len(self.checks)} checks)"
