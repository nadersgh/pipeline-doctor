import json
from typing import Any, Protocol

from loguru import logger
from pydantic import BaseModel, ValidationError

from pipeline_doctor.agent.reasoning.base import ReasonerError
from pipeline_doctor.observability.tracing import traced
from pipeline_doctor.observability.usage import UsageMeter

_MAX_ATTEMPTS = 2
_MAX_OUTPUT_TOKENS = 16000


class LLMClient(Protocol):
    def generate[T: BaseModel](self, system: str, prompt: str, schema: type[T]) -> T: ...


class AnthropicClient:
    """Structured output through `output_config.format`, validated against the pydantic model."""

    def __init__(self, model: str, meter: UsageMeter, client: Any | None = None) -> None:
        self._model = model
        self._meter = meter
        self._client = client

    def generate[T: BaseModel](self, system: str, prompt: str, schema: type[T]) -> T:
        client: Any = self._client or self._create_client()
        output_config = {"format": {"type": "json_schema", "schema": self._transform_schema(schema)}}
        messages: list[dict[str, Any]] = [{"role": "user", "content": prompt}]
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            with traced("llm.anthropic", {"gen_ai.system": "anthropic", "schema": schema.__name__, "attempt": attempt}):
                logger.info(f"call anthropic {self._model}")
                try:
                    response = client.messages.create(
                        model=self._model,
                        max_tokens=_MAX_OUTPUT_TOKENS,
                        system=system,
                        messages=messages,
                        output_config=output_config,
                    )
                except Exception as error:
                    raise ReasonerError(f"anthropic call failed: {error}") from error
                self._meter.record_llm_call(self._model, response.usage.input_tokens, response.usage.output_tokens)
            text = next((block.text for block in response.content if block.type == "text"), "")
            try:
                return schema.model_validate_json(text)
            except ValidationError as error:
                messages = [
                    *messages,
                    {"role": "assistant", "content": text or "(empty)"},
                    {"role": "user", "content": f"Your previous output was invalid: {error}. Try again."},
                ]
        raise ReasonerError(f"anthropic returned no valid {schema.__name__} after {_MAX_ATTEMPTS} attempts")

    def _transform_schema(self, schema: type[BaseModel]) -> dict[str, Any]:
        try:
            from anthropic import transform_schema  # noqa: PLC0415 - optional dependency
        except ImportError as error:
            raise ReasonerError("install the `llm` extra to use the Anthropic reasoner") from error
        return dict(transform_schema(schema))

    def _create_client(self) -> Any:
        try:
            import anthropic  # noqa: PLC0415 - optional dependency
        except ImportError as error:
            raise ReasonerError("install the `llm` extra to use the Anthropic reasoner") from error
        self._client = anthropic.Anthropic()
        return self._client


class OpenAIClient:
    """Structured output through JSON mode, validated against the pydantic model."""

    def __init__(self, model: str, meter: UsageMeter, client: Any | None = None) -> None:
        self._model = model
        self._meter = meter
        self._client = client

    def generate[T: BaseModel](self, system: str, prompt: str, schema: type[T]) -> T:
        client: Any = self._client or self._create_client()
        instructions = (
            f"{system}\n\nRespond with a single JSON object that validates against this JSON schema:\n"
            f"{json.dumps(schema.model_json_schema())}"
        )
        messages: list[dict[str, str]] = [
            {"role": "system", "content": instructions},
            {"role": "user", "content": prompt},
        ]
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            with traced("llm.openai", {"gen_ai.system": "openai", "schema": schema.__name__, "attempt": attempt}):
                logger.info(f"call openai {self._model}")
                try:
                    response = client.chat.completions.create(
                        model=self._model, messages=messages, response_format={"type": "json_object"}
                    )
                except Exception as error:
                    raise ReasonerError(f"openai call failed: {error}") from error
                usage = response.usage
                if usage is not None:
                    self._meter.record_llm_call(self._model, usage.prompt_tokens, usage.completion_tokens)
            content = response.choices[0].message.content or ""
            try:
                return schema.model_validate_json(content)
            except ValidationError as error:
                messages = [
                    *messages,
                    {"role": "assistant", "content": content},
                    {"role": "user", "content": f"Invalid: {error}. Try again."},
                ]
        raise ReasonerError(f"openai returned no valid {schema.__name__} after {_MAX_ATTEMPTS} attempts")

    def _create_client(self) -> Any:
        try:
            import openai  # noqa: PLC0415 - optional dependency
        except ImportError as error:
            raise ReasonerError("install the `llm` extra to use the OpenAI reasoner") from error
        self._client = openai.OpenAI()
        return self._client
