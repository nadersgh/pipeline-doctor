import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Self

from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from pipeline_doctor.evals.report import EvalSummary
from pipeline_doctor.settings import ReasonerKind


class EvalGateError(RuntimeError):
    pass


class GateMetric(StrEnum):
    CASES = "cases"
    CATEGORY_ACCURACY = "category_accuracy"
    CULPRIT_ACCURACY = "culprit_accuracy"
    FIX_VALIDITY = "fix_validity"
    DISTRACTOR_CATEGORY_ACCURACY = "distractor_category_accuracy"

    @property
    def is_rate(self) -> bool:
        return self is not GateMetric.CASES

    def read(self, summary: EvalSummary) -> float:
        return float(getattr(summary, self.value))


class MetricRegression(BaseModel):
    model_config = ConfigDict(frozen=True)

    metric: GateMetric
    minimum: float
    actual: float

    def describe(self) -> str:
        if self.metric.is_rate:
            return f"{self.metric}: {self.actual:.2%} < {self.minimum:.2%}"
        return f"{self.metric}: {self.actual:g} < {self.minimum:g}"


class EvalGate(BaseModel):
    """Minimum scores an eval run must reach for one reasoner."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    reasoner: ReasonerKind
    minimums: dict[GateMetric, float] = Field(min_length=1)

    @model_validator(mode="after")
    def _check_ranges(self) -> Self:
        invalid = [metric for metric, value in self.minimums.items() if value < 0 or (metric.is_rate and value > 1)]
        if invalid:
            raise ValueError(f"minimums out of range: {', '.join(invalid)} (rates are 0..1, counts >= 0)")
        return self

    def find_regressions(self, summary: EvalSummary) -> list[MetricRegression]:
        return [
            MetricRegression(metric=metric, minimum=minimum, actual=metric.read(summary))
            for metric, minimum in self.minimums.items()
            if metric.read(summary) < minimum
        ]

    def enforce(self, summary: EvalSummary) -> None:
        if summary.reasoner != self.reasoner:
            raise EvalGateError(f"gate is for the {self.reasoner} reasoner, the run used {summary.reasoner}")
        regressions = self.find_regressions(summary)
        if regressions:
            lines = "\n".join(f"  - {regression.describe()}" for regression in regressions)
            raise EvalGateError(f"eval regressed on {len(regressions)} metric(s):\n{lines}")


def load_gate(path: Path) -> EvalGate:
    logger.debug(f"read eval gate {path}")
    try:
        return EvalGate.model_validate(tomllib.loads(path.read_text()))
    except (tomllib.TOMLDecodeError, ValidationError) as error:
        raise EvalGateError(f"invalid eval gate {path}: {error}") from error
