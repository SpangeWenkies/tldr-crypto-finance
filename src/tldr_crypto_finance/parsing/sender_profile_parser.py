"""Sender-specific parsing profile selection."""

from __future__ import annotations

from dataclasses import dataclass

from tldr_crypto_finance.utils.text import slugify


@dataclass(slots=True)
class SenderProfile:
    """Sender-specific parsing hints loaded from configuration."""

    key: str
    newsletter_name: str | None
    sender_matches: list[str]
    section_patterns: list[str]
    article_split_min_words: int


class SenderProfileCatalog:
    """Catalog of sender profiles with default fallback behavior."""

    def __init__(self, profiles: dict[str, SenderProfile], default_profile: SenderProfile) -> None:
        """Store all configured sender profiles and the default fallback."""

        self.profiles = profiles
        self.default_profile = default_profile

    @classmethod
    def from_config(cls, payload: dict) -> SenderProfileCatalog:
        """Build a profile catalog from the YAML configuration mapping."""

        raw_profiles = payload.get("profiles", {})
        profiles: dict[str, SenderProfile] = {}
        for key, value in raw_profiles.items():
            profiles[key] = SenderProfile(
                key=key,
                newsletter_name=value.get("newsletter_name"),
                sender_matches=[item.lower() for item in value.get("sender_matches", [])],
                section_patterns=value.get("section_patterns", []),
                article_split_min_words=int(value.get("article_split_min_words", 25)),
            )
        default_profile = profiles.get(
            "default",
            SenderProfile(
                key="default",
                newsletter_name=None,
                sender_matches=[],
                section_patterns=[],
                article_split_min_words=25,
            ),
        )
        return cls(profiles=profiles, default_profile=default_profile)

    def select(self, sender_email: str | None, sender_name: str | None) -> SenderProfile:
        """Select the best matching sender profile for a raw message."""

        haystack = " ".join(filter(None, [sender_email, sender_name])).lower()
        for key, profile in self.profiles.items():
            if key == "default":
                continue
            if any(match in haystack for match in profile.sender_matches):
                return profile
        return self.default_profile

    def newsletter_identity(
        self,
        profile: SenderProfile,
        sender_name: str | None,
        sender_email: str | None,
    ) -> tuple[str, str]:
        """Return the display name and slug used for a parsed issue."""

        newsletter_name = (
            profile.newsletter_name or sender_name or sender_email or "Unknown Newsletter"
        )
        return newsletter_name, slugify(newsletter_name)
