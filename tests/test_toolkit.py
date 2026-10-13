from datetime import UTC, datetime

import pytest

from pipeline_doctor.agent.toolkit import _is_stale
from pipeline_doctor.incidents.models import FailureEvent, FailureSource


@pytest.mark.parametrize(
    ("generated_at", "expected"),
    [
        (datetime(2026, 10, 13, 9, 0), True),
        (datetime(2026, 10, 13, 11, 0), False),
        (datetime(2026, 10, 13, 11, 0, tzinfo=UTC), False),
    ],
)
def test_is_stale_compares_naive_and_aware_timestamps_in_utc(generated_at: datetime, expected: bool) -> None:
    # Setup
    event = FailureEvent(source=FailureSource.AIRFLOW, run_started_at=datetime(2026, 10, 13, 10, 0))

    # Act / Assert
    assert _is_stale(generated_at, event) is expected
