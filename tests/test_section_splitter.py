from tldr_crypto_finance.parsing.section_splitter import split_sections


def test_split_sections_uses_heading_heuristics() -> None:
    text = """
    Macro

    Funding stress is back in focus after weaker bank earnings.

    Rates & FX

    Traders cut ECB easing bets after a hotter inflation print.
    """
    sections = split_sections(text, [])
    assert [section.name for section in sections] == ["Macro", "Rates & FX"]


def test_split_sections_falls_back_to_general() -> None:
    sections = split_sections("One long section without headings.", [])
    assert sections[0].name == "General"
