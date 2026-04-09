"""Taxonomy loading and validation."""

from __future__ import annotations

from dataclasses import dataclass

from tldr_crypto_finance.config import Settings, load_yaml_config


@dataclass(slots=True)
class Taxonomy:
    """Configured taxonomy used for rule and optional zero-shot labeling."""

    topics: dict[str, list[str]]
    asset_classes: set[str]
    risk_types: set[str]
    regions: set[str]
    tones: set[str]

    def has_topic(self, topic: str | None) -> bool:
        """Return True when a topic is present in the configured taxonomy."""

        return bool(topic) and topic in self.topics

    def has_subtopic(self, topic: str | None, subtopic: str | None) -> bool:
        """Return True when a subtopic belongs to the given configured topic."""

        return bool(topic and subtopic) and subtopic in self.topics.get(topic, [])


def load_taxonomy(settings: Settings) -> Taxonomy:
    """Load the YAML taxonomy configuration into a typed object."""

    payload = load_yaml_config(settings.config_path("taxonomy.yml"))
    topics = {
        key: list(value.get("subtopics", []))
        for key, value in payload.get("topics", {}).items()
    }
    return Taxonomy(
        topics=topics,
        asset_classes=set(payload.get("asset_classes", [])),
        risk_types=set(payload.get("risk_types", [])),
        regions=set(payload.get("regions", [])),
        tones=set(payload.get("tones", [])),
    )
