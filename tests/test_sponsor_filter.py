from tldr_crypto_finance.parsing.sponsor_filter import detect_sponsorship

RULES = {
    "keywords": ["sponsored", "presented by"],
    "affiliate_phrases": ["open an account today"],
    "sponsor_domains": ["public.com"],
    "ambiguous_keywords": ["partner"],
}


def test_detect_sponsorship_flags_explicit_promo() -> None:
    decision = detect_sponsorship(
        "Sponsored\n\nOpen an account today with a referral bonus.\n\nhttps://public.com/promo",
        [
            {
                "original_url": "https://public.com/promo",
                "canonical_url": "https://public.com/promo",
                "domain": "public.com",
                "link_text": None,
                "link_order": 1,
            }
        ],
        RULES,
    )
    assert decision.is_sponsored is True
    assert decision.confidence >= 0.7


def test_detect_sponsorship_marks_ambiguous_partner_language() -> None:
    decision = detect_sponsorship(
        "In partner coverage today we look at custody software vendors.",
        [],
        RULES,
    )
    assert decision.is_sponsored is False
    assert decision.ambiguous is True


def test_detect_sponsorship_does_not_match_read_as_ad() -> None:
    decision = detect_sponsorship(
        "Traders cut ECB easing bets after a hotter inflation print.\n\nRead: https://macro.example.com",
        [],
        {**RULES, "keywords": ["ad"]},
    )
    assert decision.is_sponsored is False
