"""Embedding backends for search and duplicate support."""

from __future__ import annotations

import json
import math
from functools import lru_cache

from tldr_crypto_finance.utils.hashing import stable_hash
from tldr_crypto_finance.utils.text import simple_tokens


def _normalize_vector(vector: list[float]) -> list[float]:
    """Scale a vector to unit length for cosine-based similarity."""

    magnitude = math.sqrt(sum(value * value for value in vector)) or 1.0
    return [value / magnitude for value in vector]


def hashed_embedding(text: str, dimensions: int = 128) -> list[float]:
    """Create a deterministic lightweight embedding without external models."""

    vector = [0.0] * dimensions
    for token in simple_tokens(text):
        slot = int(stable_hash("slot", token), 16) % dimensions
        sign = -1.0 if int(stable_hash("sign", token), 16) % 2 else 1.0
        vector[slot] += sign
    return _normalize_vector(vector)


@lru_cache(maxsize=2)
def _load_sentence_transformer(model_name: str):
    """Load a sentence-transformer model lazily."""

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Sentence-transformer embeddings require the optional ml dependencies: "
            "pip install -e '.[ml]'"
        ) from exc
    return SentenceTransformer(model_name)


def sentence_transformer_embedding(text: str, model_name: str) -> list[float]:
    """Encode text with a sentence-transformer model."""

    model = _load_sentence_transformer(model_name)
    vector = model.encode([text], normalize_embeddings=True)[0]
    return [float(value) for value in vector]


def build_embedding(text: str, backend: str, model_name: str) -> tuple[str, list[float]]:
    """Build an embedding vector with the configured backend."""

    if backend == "sentence-transformer":
        return model_name, sentence_transformer_embedding(text, model_name)
    if backend == "hash":
        return "hash-128-v1", hashed_embedding(text)
    msg = f"Unsupported embedding backend: {backend}"
    raise ValueError(msg)


def dumps_vector(vector: list[float]) -> str:
    """Serialize an embedding vector for storage in DuckDB."""

    return json.dumps(vector)


def loads_vector(payload: str) -> list[float]:
    """Deserialize an embedding vector stored as JSON text."""

    return [float(value) for value in json.loads(payload)]
