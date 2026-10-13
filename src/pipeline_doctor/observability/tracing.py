import json
import os
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path

from loguru import logger
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SimpleSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.trace import Span, Status, StatusCode

_TRACER_NAME = "pipeline_doctor"
_configure_lock = threading.Lock()

type SpanAttribute = str | int | float | bool


class JsonLinesSpanExporter(SpanExporter):
    """Appends finished spans to a JSONL file so every run can be inspected without a tracing backend."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        lines = [json.dumps(_span_to_record(span)) for span in spans]
        with self._lock, self._path.open("a") as handle:
            handle.write("".join(f"{line}\n" for line in lines))
        return SpanExportResult.SUCCESS

    def shutdown(self) -> None:
        return None


def configure_tracing(trace_file: Path) -> None:
    """Installs the process-wide tracer provider once.

    Spans always go to a local JSONL file; they are also exported over OTLP (Langfuse, Jaeger, Honeycomb...)
    when the standard OTEL_EXPORTER_OTLP_ENDPOINT variables are set.
    """
    with _configure_lock:
        if isinstance(trace.get_tracer_provider(), TracerProvider):
            return
        provider = TracerProvider(resource=Resource.create({"service.name": "pipeline-doctor"}))
        provider.add_span_processor(SimpleSpanProcessor(JsonLinesSpanExporter(trace_file)))
        if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT") or os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"):
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter  # noqa: PLC0415

            logger.info("export traces to OTLP endpoint")
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(provider)


@contextmanager
def traced(name: str, attributes: dict[str, SpanAttribute] | None = None) -> Iterator[Span]:
    tracer = trace.get_tracer(_TRACER_NAME)
    with tracer.start_as_current_span(name, attributes=attributes or {}) as span:
        try:
            yield span
        except Exception as error:
            span.set_status(Status(StatusCode.ERROR, str(error)))
            raise


def _span_to_record(span: ReadableSpan) -> dict[str, object]:
    context = span.get_span_context()
    duration_ms = ((span.end_time or 0) - (span.start_time or 0)) / 1_000_000
    return {
        "trace_id": f"{context.trace_id:032x}" if context else None,
        "span_id": f"{context.span_id:016x}" if context else None,
        "parent_span_id": f"{span.parent.span_id:016x}" if span.parent else None,
        "name": span.name,
        "duration_ms": round(duration_ms, 2),
        "status": span.status.status_code.name,
        "attributes": dict(span.attributes or {}),
    }
