import hashlib
import itertools
import re
from typing import Protocol

import numpy as np
from numpy.typing import NDArray

_TOKEN = re.compile(r"[a-z0-9_]+")


class TextEmbedder(Protocol):
    @property
    def dimensions(self) -> int: ...

    def embed(self, text: str) -> NDArray[np.float32]: ...


class HashingEmbedder:
    """Deterministic, dependency-free embedding: signed feature hashing of unigrams and bigrams."""

    def __init__(self, dimensions: int = 512) -> None:
        self._dimensions = dimensions

    @property
    def dimensions(self) -> int:
        return self._dimensions

    def embed(self, text: str) -> NDArray[np.float32]:
        tokens = _TOKEN.findall(text.lower())
        features = tokens + [f"{left} {right}" for left, right in itertools.pairwise(tokens)]
        vector = np.zeros(self._dimensions, dtype=np.float32)
        for feature in features:
            digest = hashlib.blake2b(feature.encode(), digest_size=8).digest()
            index = int.from_bytes(digest[:4], "little") % self._dimensions
            vector[index] += 1.0 if digest[4] & 1 else -1.0
        norm = float(np.linalg.norm(vector))
        return vector / norm if norm else vector
