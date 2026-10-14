import json
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from pipeline_doctor.agent.context import IncidentContext
from pipeline_doctor.agent.reasoning.base import Assessment, FallbackReasoner, Hypothesis, Investigation, ReasonerError
from pipeline_doctor.agent.reasoning.llm import AssessmentDraft, HypothesisList, LLMReasoner
from pipeline_doctor.agent.reasoning.llm_clients import AnthropicClient, OpenAIClient
from pipeline_doctor.incidents.models import (
    Culprit,
    FailureEvent,
    FailureSource,
    FixKind,
    FixProposal,
    RootCauseCategory,
    RunMetrics,
)
from pipeline_doctor.observability.usage import UsageMeter

_HYPOTHESES = {
    "hypotheses": [
        {
            "category": "source_null_spike",
            "culprit": {"relation": "raw.orders", "column": "customer_id"},
            "rationale": "not_null failed",
            "checks": [{"kind": "null_rate_by_load_day", "relation": "raw.orders", "column": "customer_id"}],
        }
    ]
}


class ScriptedLLMClient:
    def __init__(self, responses: dict[str, dict[str, Any]]) -> None:
        self._responses = responses
        self.prompts: list[str] = []

    def generate[T: BaseModel](self, system: str, prompt: str, schema: type[T]) -> T:
        self.prompts.append(prompt)
        return schema.model_validate(self._responses[schema.__name__])


class FailingReasoner:
    @property
    def name(self) -> str:
        return "failing"

    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]:
        raise ReasonerError("provider outage")

    def assess(self, investigation: Investigation) -> Assessment:
        raise ReasonerError("provider outage")

    def propose_fix(self, investigation: Investigation, feedback: str) -> FixProposal:
        raise ReasonerError("provider outage")


class StaticReasoner(FailingReasoner):
    def hypothesize(self, investigation: Investigation) -> list[Hypothesis]:
        return [Hypothesis(category=RootCauseCategory.UNKNOWN, culprit=Culprit(relation="x"), rationale="fallback")]


@pytest.fixture
def investigation() -> Investigation:
    event = FailureEvent(source=FailureSource.AIRFLOW, dag_id="d", task_id="t", run_id="r")
    return Investigation(context=IncidentContext(event=event))


@pytest.fixture
def meter() -> UsageMeter:
    return UsageMeter(input_cost_per_mtok=3.0, output_cost_per_mtok=15.0)


def test_llm_reasoner_parses_structured_hypotheses(investigation: Investigation) -> None:
    # Setup
    client = ScriptedLLMClient({HypothesisList.__name__: _HYPOTHESES})

    # Act
    hypotheses = LLMReasoner(client, "fake", "duckdb").hypothesize(investigation)

    # Assert
    assert hypotheses[0].category is RootCauseCategory.SOURCE_NULL_SPIKE
    assert hypotheses[0].checks[0].column == "customer_id"
    assert "Investigation round 1" in client.prompts[0]


def test_llm_reasoner_assessment_keeps_follow_up_checks(investigation: Investigation) -> None:
    # Setup
    draft = {
        "ready": False,
        "diagnosis": {
            "category": "source_null_spike",
            "confidence": 0.4,
            "culprit": {"relation": "raw.orders", "column": "customer_id"},
            "technical_summary": "t",
            "plain_summary": "p",
        },
        "follow_up_checks": [{"kind": "volume_by_load_day", "relation": "raw.orders"}],
    }
    client = ScriptedLLMClient({AssessmentDraft.__name__: draft})

    # Act
    assessment = LLMReasoner(client, "fake", "duckdb").assess(investigation)

    # Assert
    assert not assessment.ready
    assert assessment.follow_up_checks[0].relation == "raw.orders"


def test_fallback_reasoner_uses_fallback_when_primary_fails(investigation: Investigation) -> None:
    # Act
    hypotheses = FallbackReasoner(FailingReasoner(), StaticReasoner()).hypothesize(investigation)

    # Assert
    assert hypotheses[0].rationale == "fallback"


def test_fallback_reasoner_counts_each_fallback_in_the_run_metrics(
    investigation: Investigation, meter: UsageMeter
) -> None:
    # Setup
    reasoner = FallbackReasoner(FailingReasoner(), StaticReasoner(), meter)

    # Act
    reasoner.hypothesize(investigation)
    reasoner.hypothesize(investigation)

    # Assert
    assert meter.fill(RunMetrics()).llm_fallbacks == 2


def _anthropic_response(payload: dict[str, Any] | str) -> SimpleNamespace:
    return SimpleNamespace(
        content=[SimpleNamespace(type="text", text=payload if isinstance(payload, str) else json.dumps(payload))],
        usage=SimpleNamespace(input_tokens=1_000, output_tokens=200),
    )


def test_anthropic_client_retries_invalid_output_and_meters_cost(meter: UsageMeter) -> None:
    # Setup
    responses = [_anthropic_response({"hypotheses": "not a list"}), _anthropic_response(_HYPOTHESES)]
    sdk = SimpleNamespace(messages=SimpleNamespace(create=lambda **_: responses.pop(0)))
    client = AnthropicClient("claude-test", meter, client=sdk)

    # Act
    result = client.generate("system", "prompt", HypothesisList)
    metrics = meter.fill(RunMetrics())

    # Assert
    assert result.hypotheses[0].culprit.relation == "raw.orders"
    assert metrics.llm_calls == 2
    assert metrics.cost_usd == pytest.approx(2 * (1_000 * 3.0 + 200 * 15.0) / 1_000_000)


def test_anthropic_client_requests_json_schema_output_without_forcing_a_tool(meter: UsageMeter) -> None:
    # Setup
    requests: list[dict[str, Any]] = []

    def create(**request: Any) -> SimpleNamespace:
        requests.append(request)
        return _anthropic_response(_HYPOTHESES)

    client = AnthropicClient("claude-test", meter, client=SimpleNamespace(messages=SimpleNamespace(create=create)))

    # Act
    client.generate("system", "prompt", HypothesisList)

    # Assert
    assert "tool_choice" not in requests[0]
    assert "tools" not in requests[0]
    output_format = requests[0]["output_config"]["format"]
    assert output_format["type"] == "json_schema"
    assert output_format["schema"]["additionalProperties"] is False


def test_anthropic_client_wraps_provider_errors(meter: UsageMeter) -> None:
    # Setup
    def fail(**_: Any) -> None:
        raise ConnectionError("network down")

    client = AnthropicClient("claude-test", meter, client=SimpleNamespace(messages=SimpleNamespace(create=fail)))

    # Act / Assert
    with pytest.raises(ReasonerError, match="network down"):
        client.generate("system", "prompt", HypothesisList)


def test_openai_client_validates_json_mode_output(meter: UsageMeter) -> None:
    # Setup
    response = SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(_HYPOTHESES)))],
        usage=SimpleNamespace(prompt_tokens=500, completion_tokens=100),
    )
    sdk = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(create=lambda **_: response)))

    # Act
    result = OpenAIClient("gpt-test", meter, client=sdk).generate("system", "prompt", HypothesisList)

    # Assert
    assert result.hypotheses[0].category is RootCauseCategory.SOURCE_NULL_SPIKE
    assert meter.fill(RunMetrics()).input_tokens == 500


def test_llm_reasoner_fix_prompt_includes_feedback(investigation: Investigation) -> None:
    # Setup
    fix = {"kind": "operational", "title": "Escalate", "rationale": "upstream", "operational_steps": ["page owner"]}
    client = ScriptedLLMClient({"FixDraft": fix})

    # Act
    proposal = LLMReasoner(client, "fake", "duckdb").propose_fix(investigation, feedback="snippet not found")

    # Assert
    assert proposal.kind is FixKind.OPERATIONAL
    assert "snippet not found" in client.prompts[0]
