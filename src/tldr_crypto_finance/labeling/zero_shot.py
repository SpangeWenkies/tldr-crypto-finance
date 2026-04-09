"""Optional zero-shot topic classification."""

from __future__ import annotations

from functools import lru_cache

from tldr_crypto_finance.labeling.taxonomy import Taxonomy


@lru_cache(maxsize=2)
def _load_pipeline(model_name: str):
    """Load the transformers zero-shot pipeline on demand."""

    try:
        from transformers import pipeline
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "Zero-shot labeling requires the optional ml dependencies: pip install -e '.[ml]'"
        ) from exc
    return pipeline("zero-shot-classification", model=model_name)


def zero_shot_topic_label(
    text: str,
    taxonomy: Taxonomy,
    model_name: str,
) -> tuple[str | None, str | None, float]:
    """Classify a topic and optional subtopic with a zero-shot model."""

    classifier = _load_pipeline(model_name)
    topic_candidates = list(taxonomy.topics.keys())
    topic_result = classifier(text, candidate_labels=topic_candidates, multi_label=False)
    topic = topic_result["labels"][0]
    topic_score = float(topic_result["scores"][0])
    subtopic = None
    subtopic_score = 0.0
    if taxonomy.topics.get(topic):
        subtopic_result = classifier(
            text,
            candidate_labels=taxonomy.topics[topic],
            multi_label=False,
        )
        subtopic = subtopic_result["labels"][0]
        subtopic_score = float(subtopic_result["scores"][0])
    return topic, subtopic, min(topic_score, subtopic_score or topic_score)
