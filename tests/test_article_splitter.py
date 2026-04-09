from tldr_crypto_finance.parsing.article_splitter import extract_title, split_article_blocks


def test_split_article_blocks_splits_on_linked_story_boundaries() -> None:
    text = """
    Stablecoins wobble after a reserve disclosure.

    Read more: https://example.com/stablecoins

    Bank funding stress rises after a deposit outflow warning.

    More: https://example.com/funding
    """
    blocks = split_article_blocks(text)
    assert len(blocks) == 2


def test_extract_title_prefers_short_heading_like_first_paragraph() -> None:
    block = "Stablecoins Under Pressure\n\nReserve disclosure raised fresh questions."
    title, confidence = extract_title(block)
    assert title == "Stablecoins Under Pressure"
    assert confidence >= 0.8
