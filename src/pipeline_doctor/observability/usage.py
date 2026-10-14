import threading

from opentelemetry import trace

from pipeline_doctor.incidents.models import RunMetrics


class UsageMeter:
    """Accumulates LLM token usage and cost for one agent run and stamps it on the active span."""

    def __init__(self, input_cost_per_mtok: float, output_cost_per_mtok: float) -> None:
        self._input_cost_per_mtok = input_cost_per_mtok
        self._output_cost_per_mtok = output_cost_per_mtok
        self._lock = threading.Lock()
        self._llm_calls = 0
        self._llm_fallbacks = 0
        self._input_tokens = 0
        self._output_tokens = 0

    def record_llm_call(self, model: str, input_tokens: int, output_tokens: int) -> None:
        with self._lock:
            self._llm_calls += 1
            self._input_tokens += input_tokens
            self._output_tokens += output_tokens
        span = trace.get_current_span()
        span.set_attribute("gen_ai.request.model", model)
        span.set_attribute("gen_ai.usage.input_tokens", input_tokens)
        span.set_attribute("gen_ai.usage.output_tokens", output_tokens)

    def record_fallback(self) -> None:
        with self._lock:
            self._llm_fallbacks += 1

    def fill(self, metrics: RunMetrics) -> RunMetrics:
        cost = (
            self._input_tokens * self._input_cost_per_mtok + self._output_tokens * self._output_cost_per_mtok
        ) / 1_000_000
        return metrics.model_copy(
            update={
                "llm_calls": self._llm_calls,
                "llm_fallbacks": self._llm_fallbacks,
                "input_tokens": self._input_tokens,
                "output_tokens": self._output_tokens,
                "cost_usd": round(cost, 6),
            }
        )
