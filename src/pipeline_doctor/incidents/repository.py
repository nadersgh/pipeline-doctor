import fcntl
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from loguru import logger
from pydantic import ValidationError

from pipeline_doctor.incidents.models import Incident

_INCIDENT_ID = re.compile(r"[A-Za-z0-9]+")


class IncidentNotFoundError(LookupError):
    pass


class IncidentRepository:
    """Stores incidents as one JSON document per incident."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        self._directory.mkdir(parents=True, exist_ok=True)

    def save(self, incident: Incident) -> Incident:
        stamped = incident.model_copy(update={"updated_at": datetime.now(UTC)})
        path = self._path_for(stamped.id)
        logger.debug(f"write incident {path}")
        temporary = path.with_name(f"{path.stem}.{uuid4().hex}.tmp")
        temporary.write_text(stamped.model_dump_json(indent=2))
        os.replace(temporary, path)
        return stamped

    @contextmanager
    def lock(self, incident_id: str) -> Iterator[None]:
        """Holds an exclusive advisory lock on one incident across threads and processes on the same host."""
        lock_path = self._path_for(incident_id).with_suffix(".lock")
        logger.debug(f"lock incident {lock_path}")
        with lock_path.open("a") as handle:
            fcntl.flock(handle, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def get(self, incident_id: str) -> Incident:
        path = self._path_for(incident_id)
        if not path.exists():
            raise IncidentNotFoundError(incident_id)
        logger.debug(f"read incident {path}")
        return Incident.model_validate_json(path.read_text())

    def list_all(self) -> list[Incident]:
        logger.debug(f"read incidents {self._directory}")
        incidents = [incident for path in self._directory.glob("*.json") if (incident := self._read_valid(path))]
        return sorted(incidents, key=lambda incident: incident.created_at, reverse=True)

    def _read_valid(self, path: Path) -> Incident | None:
        try:
            return Incident.model_validate_json(path.read_text())
        except (ValidationError, OSError):
            logger.exception(f"skip unreadable incident {path}")
            return None

    def _path_for(self, incident_id: str) -> Path:
        if not _INCIDENT_ID.fullmatch(incident_id):
            raise IncidentNotFoundError(incident_id)
        return self._directory / f"{incident_id}.json"
