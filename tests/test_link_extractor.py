from tldr_crypto_finance.parsing.link_extractor import extract_links


def test_extract_links_deduplicates_and_canonicalizes() -> None:
    links = extract_links(
        """
        Read more: https://example.com/story?utm_source=newsletter
        Same story again: https://example.com/story?utm_source=another
        Different story: https://example.com/other
        """
    )
    assert len(links) == 2
    assert links[0]["canonical_url"] == "https://example.com/story"
    assert links[1]["domain"] == "example.com"
