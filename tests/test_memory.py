from pathlib import Path

from pipeline_doctor.incidents.models import RootCauseCategory
from pipeline_doctor.memory.store import IncidentMemory, PastIncident


def _incident(incident_id: str, signature: str, category: RootCauseCategory) -> PastIncident:
    return PastIncident(
        incident_id=incident_id, signature=signature, category=category, culprit="raw.orders", resolution="fixed"
    )


def test_incident_memory_ranks_the_most_similar_failure_first(tmp_path: Path) -> None:
    # Setup
    memory = IncidentMemory(tmp_path / "memory.jsonl")
    memory.remember(
        _incident("a", "test unique stg_orders order_id duplicate keys", RootCauseCategory.SOURCE_DUPLICATE_ROWS)
    )
    memory.remember(
        _incident(
            "b", "model stg_payments binder error column amount not found", RootCauseCategory.SOURCE_COLUMN_RENAMED
        )
    )

    # Act
    results = memory.search("model stg_payments binder error referenced column method not found")

    # Assert
    assert results[0].incident.incident_id == "b"


def test_incident_memory_persists_across_instances(tmp_path: Path) -> None:
    # Setup
    path = tmp_path / "memory.jsonl"
    IncidentMemory(path).remember(_incident("a", "freshness error raw orders", RootCauseCategory.SOURCE_STALE))

    # Act
    reloaded = IncidentMemory(path)

    # Assert
    assert len(reloaded) == 1
    assert reloaded.search("freshness error raw orders")[0].similarity > 0.99


def test_incident_memory_search_on_empty_store_returns_nothing(tmp_path: Path) -> None:
    assert IncidentMemory(tmp_path / "memory.jsonl").search("anything") == []
