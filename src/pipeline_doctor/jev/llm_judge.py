from pydantic import BaseModel, Field

from pipeline_doctor.agent.reasoning.llm_clients import LLMClient
from pipeline_doctor.jev.models import JevCheck, JevStage, JevVerdict
from pipeline_doctor.jev.reviewer import JevRequest, decide

JUDGE_SYSTEM_PROMPT = """You are the JEV reviewer for Pipeline Doctor: a skeptical senior data engineer who reviews
another agent's incident diagnosis and proposed fix before a human sees it.

Default to rejecting. Approve only what the evidence shows. You never see the ground truth; judge only from the
gathered context, the diagnostic query results, and the patch."""

JUDGE_INSTRUCTIONS = """{context}

## Diagnostic evidence
{evidence}

## Diagnosis under review
{diagnosis}

## Proposed fix ({kind}): {title}
{rationale}

```diff
{diff}
```
Operational steps: {steps}

Answer three questions independently:
1. diagnosis_supported: do the evidence and context point to this category and culprit over the alternatives?
   Watch for distractor commits (docs, comments, unrelated models) blamed for upstream data problems.
2. fix_addresses_cause: does the fix remove the root cause rather than hide the symptom
   (e.g. deleting or loosening a test, filtering rows silently)?
3. fix_is_safe: is the patch minimal, read-only at runtime, and limited to the failing lineage?
List each concrete concern in `concerns`."""


class JudgeOpinion(BaseModel):
    diagnosis_supported: bool
    fix_addresses_cause: bool
    fix_is_safe: bool
    concerns: list[str] = Field(default_factory=list)


class LLMJudge:
    """LLM-as-judge reviewer; used behind the rule gate so it can only make JEV stricter."""

    def __init__(self, client: LLMClient, model_label: str) -> None:
        self._client = client
        self._model_label = model_label

    @property
    def name(self) -> str:
        return f"llm_judge:{self._model_label}"

    def review(self, request: JevRequest) -> JevVerdict:
        investigation, fix = request.investigation, request.fix
        diagnosis = investigation.diagnosis
        prompt = JUDGE_INSTRUCTIONS.format(
            context=investigation.context.render(),
            evidence=investigation.render_evidence(),
            diagnosis=diagnosis.model_dump_json(indent=2) if diagnosis else "none",
            kind=fix.kind,
            title=fix.title,
            rationale=fix.rationale,
            diff=fix.diff or "(no code change)",
            steps=fix.operational_steps or "none",
        )
        opinion = self._client.generate(JUDGE_SYSTEM_PROMPT, prompt, JudgeOpinion)
        concerns = "; ".join(opinion.concerns) or "no concerns"
        checks = [
            JevCheck(
                stage=JevStage.JUDGE,
                name="llm_diagnosis_supported",
                passed=opinion.diagnosis_supported,
                detail=concerns,
            ),
            JevCheck(
                stage=JevStage.EVALUATE,
                name="llm_fix_addresses_cause",
                passed=opinion.fix_addresses_cause,
                detail=concerns,
            ),
            JevCheck(stage=JevStage.VERIFY, name="llm_fix_is_safe", passed=opinion.fix_is_safe, detail=concerns),
        ]
        return decide(checks, request, self.name)
