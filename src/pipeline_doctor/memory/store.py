import json
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
from loguru import logger
from pydantic import BaseModel, Field

from pipeline_doctor.incidents.models import RootCauseCategory
from pipeline_doctor.memory.embedder import HashingEmbedder, TextEmbedder


class PastIncident(BaseModel):
    incident_id: str
    signature: str = Field(description="Failure fingerprint: failing node, test, and error message.")
    category: RootCauseCategory
    culprit: str
    resolution: str
    resolved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SimilarIncident(BaseModel):
    incident: PastIncident
    similarity: float


class IncidentMemory:
    """Vector store of resolved incidents persisted as JSON lines alongside their embeddings."""

    def __init__(self, path: Path, embedder: TextEmbedder | None = None) -> None:
        self._path = path
        self._embedder = embedder or HashingEmbedder()
        self._incidents: list[PastIncident] = []
        self._vectors = np.zeros((0, self._embedder.dimensions), dtype=np.float32)
        self._load()

    def __len__(self) -> int:
        return len(self._incidents)

    def remember(self, incident: PastIncident) -> None:
        self._incidents.append(incident)
        self._vectors = np.vstack([self._vectors, self._embedder.embed(incident.signature)])
        self._path.parent.mkdir(parents=True, exist_ok=True)
        logger.debug(f"append incident memory {self._path}")
        with self._path.open("a") as handle:
            handle.write(incident.model_dump_json() + "\n")

    def search(self, signature: str, limit: int = 3, min_similarity: float = 0.35) -> list[SimilarIncident]:
        if not self._incidents:
            return []
        scores = self._vectors @ self._embedder.embed(signature)
        ranked = np.argsort(-scores)[:limit]
        return [
            SimilarIncident(incident=self._incidents[index], similarity=round(float(scores[index]), 3))
            for index in ranked
            if scores[index] >= min_similarity
        ]

    def _load(self) -> None:
        if not self._path.exists():
            return
        logger.debug(f"read incident memory {self._path}")
        self._incidents = [
            PastIncident.model_validate(json.loads(line))
            for line in self._path.read_text().splitlines()
            if line.strip()
        ]
        if self._incidents:
            self._vectors = np.vstack([self._embedder.embed(incident.signature) for incident in self._incidents])
