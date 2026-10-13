from pathlib import Path

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from typer.testing import CliRunner

from pipeline_doctor.cli import app
from pipeline_doctor.evals.gate import EvalGate, EvalGateError, GateMetric, load_gate
from pipeline_doctor.evals.report import EvalSummary

_COMMITTED_GATE = Path(__file__).parents[1] / "evals" / "gate.toml"
_GATE_TOML = """
reasoner = "heuristic"

[minimums]
cases = 36
category_accuracy = 1.0
culprit_accuracy = 1.0
fix_validity = 1.0
distractor_category_accuracy = 1.0
"""


class EvalSummaryFactory(ModelFactory[EvalSummary]):
    __model__ = EvalSummary
    reasoner = "heuristic"
    cases = 36
    triggered = 36
    category_accuracy = 1.0
    culprit_accuracy = 1.0
    fix_validity = 1.0
    distractor_category_accuracy = 1.0


@pytest.fixture
def gate(tmp_path: Path) -> EvalGate:
    path = tmp_path / "gate.toml"
    path.write_text(_GATE_TOML)
    return load_gate(path)


def test_committed_gate_passes_on_a_perfect_heuristic_run() -> None:
    # Setup
    committed = load_gate(_COMMITTED_GATE)

    # Act
    regressions = committed.find_regressions(EvalSummaryFactory.build())

    # Assert
    assert committed.reasoner == "heuristic"
    assert set(committed.minimums) == set(GateMetric)
    assert regressions == []


@pytest.mark.parametrize(
    ("metric", "actual", "expected_line"),
    [
        (GateMetric.CASES, 35, "cases: 35 < 36"),
        (GateMetric.CATEGORY_ACCURACY, 0.9722, "category_accuracy: 97.22% < 100.00%"),
        (GateMetric.CULPRIT_ACCURACY, 0.5, "culprit_accuracy: 50.00% < 100.00%"),
        (GateMetric.FIX_VALIDITY, 0.0, "fix_validity: 0.00% < 100.00%"),
        (GateMetric.DISTRACTOR_CATEGORY_ACCURACY, 0.9, "distractor_category_accuracy: 90.00% < 100.00%"),
    ],
)
def test_gate_enforce_lists_the_dropped_metric(
    gate: EvalGate, metric: GateMetric, actual: float, expected_line: str
) -> None:
    # Setup
    summary = EvalSummaryFactory.build(**{metric.value: actual})

    # Act
    with pytest.raises(EvalGateError) as raised:
        gate.enforce(summary)

    # Assert
    assert "regressed on 1 metric(s)" in str(raised.value)
    assert f"  - {expected_line}" in str(raised.value)


def test_gate_enforce_lists_every_dropped_metric(gate: EvalGate) -> None:
    # Setup
    summary = EvalSummaryFactory.build(category_accuracy=0.9, fix_validity=0.8)

    # Act
    regressions = gate.find_regressions(summary)

    # Assert
    assert [regression.metric for regression in regressions] == [
        GateMetric.CATEGORY_ACCURACY,
        GateMetric.FIX_VALIDITY,
    ]
    with pytest.raises(EvalGateError, match=r"regressed on 2 metric\(s\)"):
        gate.enforce(summary)


def test_gate_enforce_passes_when_scores_beat_the_minimums(tmp_path: Path) -> None:
    # Setup
    path = tmp_path / "gate.toml"
    path.write_text('reasoner = "heuristic"\n[minimums]\ncategory_accuracy = 0.9\ncases = 10\n')
    summary = EvalSummaryFactory.build(category_accuracy=0.95, culprit_accuracy=0.0)

    # Act
    load_gate(path).enforce(summary)

    # Assert
    assert load_gate(path).find_regressions(summary) == []


def test_gate_enforce_rejects_a_run_from_another_reasoner(gate: EvalGate) -> None:
    # Setup
    summary = EvalSummaryFactory.build(reasoner="anthropic")

    # Act / Assert
    with pytest.raises(EvalGateError, match="gate is for the heuristic reasoner, the run used anthropic"):
        gate.enforce(summary)


@pytest.mark.parametrize(
    "content",
    [
        'reasoner = "heuristic"\n[minimums]\ncategory_accuracy = 1.5\n',
        'reasoner = "heuristic"\n[minimums]\ncases = -1\n',
        'reasoner = "heuristic"\n[minimums]\nlatency = 1.0\n',
        'reasoner = "heuristic"\n[minimums]\n',
        'reasoner = "gpt"\n[minimums]\ncases = 1\n',
        "reasoner = heuristic",
    ],
)
def test_load_gate_rejects_invalid_files(tmp_path: Path, content: str) -> None:
    # Setup
    path = tmp_path / "gate.toml"
    path.write_text(content)

    # Act / Assert
    with pytest.raises(EvalGateError, match="invalid eval gate"):
        load_gate(path)


@pytest.mark.integration
@pytest.mark.parametrize(("minimum_cases", "exit_code"), [(1, 0), (2, 1)])
def test_eval_cli_exit_code_follows_the_gate(tmp_path: Path, minimum_cases: int, exit_code: int) -> None:
    # Setup
    gate_path = tmp_path / "gate.toml"
    gate_path.write_text(f'reasoner = "heuristic"\n[minimums]\ncases = {minimum_cases}\ncategory_accuracy = 1.0\n')
    arguments = [
        "eval",
        "--reasoner",
        "heuristic",
        "--cases",
        "renamed_orders_status",
        "--workers",
        "1",
        "--work-dir",
        str(tmp_path / "work"),
        "--output-dir",
        str(tmp_path / "results"),
        "--gate",
        str(gate_path),
    ]

    # Act
    result = CliRunner().invoke(app, arguments)

    # Assert
    assert result.exit_code == exit_code, result.output
    if exit_code:
        assert isinstance(result.exception, EvalGateError)
        assert "cases: 1 < 2" in str(result.exception)
