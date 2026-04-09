"""Lightweight entity extraction for finance, technology, and risk text.

This module keeps the default path dependency-free. It uses expanded gazetteers
plus context-aware heuristics instead of a full NER model, so coverage is broad
but still heuristic rather than exhaustive.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from functools import lru_cache

ENTITY_PATTERNS = {
    "ticker": re.compile(r"(?<![A-Za-z0-9])\$?[A-Z]{2,6}(?![A-Za-z0-9])"),
}

TICKER_STOPWORDS = {
    "AI",
    "API",
    "APP",
    "AWS",
    "CEO",
    "CFO",
    "CFTC",
    "CIO",
    "COO",
    "CTO",
    "DAO",
    "ECB",
    "ETF",
    "EU",
    "FED",
    "GDP",
    "IPO",
    "IRS",
    "LLM",
    "NFT",
    "OFAC",
    "OK",
    "PCE",
    "PDF",
    "READ",
    "ROI",
    "SDK",
    "SEC",
    "SQL",
    "UI",
    "UK",
    "URL",
    "USD",
}

TICKER_ENTITY_ALIASES = {
    "AAPL": [("Apple", "company")],
    "AERO": [("Aerodrome", "amm")],
    "AFRM": [("Affirm", "company")],
    "AMZN": [("Amazon", "company")],
    "ARB": [("Arbitrum", "coin")],
    "AVAX": [("Avalanche", "coin")],
    "BAL": [("Balancer", "amm")],
    "BNB": [("BNB", "coin"), ("BNB Chain", "network")],
    "BONK": [("Bonk", "coin")],
    "BTC": [("Bitcoin", "coin")],
    "CAKE": [("PancakeSwap", "amm")],
    "COIN": [("Coinbase", "company"), ("Coinbase", "exchange")],
    "CRV": [("Curve", "amm")],
    "DAI": [("DAI", "coin")],
    "DOGE": [("Dogecoin", "coin")],
    "ETH": [("Ethereum", "coin")],
    "FDUSD": [("FDUSD", "coin")],
    "GOOG": [("Alphabet", "company")],
    "GOOGL": [("Alphabet", "company")],
    "HBAR": [("Hedera", "coin")],
    "HOOD": [("Robinhood", "company")],
    "HYPE": [("Hyperliquid", "coin"), ("Hyperliquid", "network")],
    "INJ": [("Injective", "coin"), ("Injective", "network")],
    "INTU": [("Intuit", "company")],
    "JOE": [("Trader Joe", "amm")],
    "JUP": [("Jupiter", "coin")],
    "LINK": [("Chainlink", "coin")],
    "MATIC": [("Polygon", "coin"), ("Polygon", "network")],
    "MAV": [("Maverick", "amm")],
    "MELI": [("Mercado Libre", "company")],
    "META": [("Meta", "company")],
    "MSTR": [("MicroStrategy", "company")],
    "MSFT": [("Microsoft", "company")],
    "NEAR": [("NEAR", "coin"), ("NEAR", "network")],
    "NFLX": [("Netflix", "company")],
    "NVDA": [("NVIDIA", "company")],
    "OKB": [("OKX", "exchange")],
    "OP": [("Optimism", "coin"), ("Optimism", "network")],
    "ORCA": [("Orca", "amm")],
    "PEPE": [("Pepe", "coin")],
    "PLTR": [("Palantir", "company")],
    "POL": [("Polygon", "coin"), ("Polygon", "network")],
    "PYPL": [("PayPal", "company")],
    "PYUSD": [("PayPal USD", "coin")],
    "RAY": [("Raydium", "amm")],
    "RLUSD": [("RLUSD", "coin")],
    "SEI": [("Sei", "coin"), ("Sei", "network")],
    "SHOP": [("Shopify", "company")],
    "SOFI": [("SoFi", "company")],
    "SOL": [("Solana", "coin"), ("Solana", "network")],
    "SQ": [("Block", "company")],
    "SUI": [("Sui", "coin"), ("Sui", "network")],
    "SUSHI": [("SushiSwap", "amm")],
    "TIA": [("Celestia", "coin"), ("Celestia", "network")],
    "TON": [("TON", "coin"), ("TON", "network")],
    "TRX": [("Tron", "coin"), ("Tron", "network")],
    "UNI": [("Uniswap", "amm")],
    "USDC": [("USD Coin", "coin")],
    "USDT": [("Tether", "coin")],
    "WBTC": [("Wrapped Bitcoin", "coin")],
    "WLD": [("Worldcoin", "coin")],
    "XLM": [("Stellar", "coin"), ("Stellar", "network")],
    "XRP": [("XRP", "coin"), ("XRP Ledger", "network")],
    "XYZ": [("Block", "company")],
}

ENTITY_GAZETTEERS = {
    "company": {
        "affirm": "Affirm",
        "adyen": "Adyen",
        "amazon": "Amazon",
        "anchorage": "Anchorage",
        "anthropic": "Anthropic",
        "apple": "Apple",
        "block": "Block",
        "blackrock": "BlackRock",
        "brex": "Brex",
        "chainalysis": "Chainalysis",
        "chime": "Chime",
        "circle": "Circle",
        "coinbase": "Coinbase",
        "consensys": "Consensys",
        "databricks": "Databricks",
        "fireblocks": "Fireblocks",
        "fidelity": "Fidelity",
        "galaxy": "Galaxy",
        "goldman sachs": "Goldman Sachs",
        "google": "Google",
        "intuit": "Intuit",
        "jpmorgan": "JPMorgan",
        "klarna": "Klarna",
        "kraken": "Kraken",
        "mastercard": "Mastercard",
        "mercury": "Mercury",
        "mercado libre": "Mercado Libre",
        "meta": "Meta",
        "microstrategy": "MicroStrategy",
        "microsoft": "Microsoft",
        "moonpay": "MoonPay",
        "nvidia": "NVIDIA",
        "openai": "OpenAI",
        "palantir": "Palantir",
        "paypal": "PayPal",
        "plaid": "Plaid",
        "ramp": "Ramp",
        "revolut": "Revolut",
        "robinhood": "Robinhood",
        "shopify": "Shopify",
        "sofi": "SoFi",
        "square": "Square",
        "stripe": "Stripe",
        "tether": "Tether",
        "visa": "Visa",
        "wise": "Wise",
    },
    "exchange": {
        "binance": "Binance",
        "bybit": "Bybit",
        "cboe": "Cboe",
        "cme": "CME",
        "coinbase": "Coinbase",
        "deribit": "Deribit",
        "hyperliquid": "Hyperliquid",
        "kraken": "Kraken",
        "lse": "LSE",
        "nasdaq": "Nasdaq",
        "nyse": "NYSE",
        "okx": "OKX",
    },
    "regulator": {
        "apra": "APRA",
        "asic": "ASIC",
        "bafin": "BaFin",
        "bank of england": "Bank of England",
        "bis": "BIS",
        "cftc": "CFTC",
        "cfpb": "CFPB",
        "doj": "DOJ",
        "eba": "EBA",
        "ecb": "ECB",
        "esma": "ESMA",
        "fca": "FCA",
        "fdic": "FDIC",
        "fincen": "FinCEN",
        "finma": "FINMA",
        "fsa": "FSA",
        "hkma": "HKMA",
        "mas": "MAS",
        "occ": "OCC",
        "ofac": "OFAC",
        "pboc": "PBOC",
        "pra": "PRA",
        "sec": "SEC",
        "sfc": "SFC",
        "u.s. treasury": "U.S. Treasury",
    },
    "country": {
        "argentina": "Argentina",
        "australia": "Australia",
        "brazil": "Brazil",
        "canada": "Canada",
        "china": "China",
        "france": "France",
        "germany": "Germany",
        "hong kong": "Hong Kong",
        "india": "India",
        "indonesia": "Indonesia",
        "ireland": "Ireland",
        "israel": "Israel",
        "italy": "Italy",
        "japan": "Japan",
        "mexico": "Mexico",
        "netherlands": "Netherlands",
        "nigeria": "Nigeria",
        "singapore": "Singapore",
        "south korea": "South Korea",
        "spain": "Spain",
        "switzerland": "Switzerland",
        "taiwan": "Taiwan",
        "turkey": "Turkey",
        "uae": "UAE",
        "uk": "UK",
        "united arab emirates": "United Arab Emirates",
        "united kingdom": "United Kingdom",
        "united states": "United States",
        "vietnam": "Vietnam",
        "american": "United States",
        "argentine": "Argentina",
        "argentinian": "Argentina",
        "australian": "Australia",
        "brazilian": "Brazil",
        "british": "United Kingdom",
        "canadian": "Canada",
        "chinese": "China",
        "french": "France",
        "german": "Germany",
        "indian": "India",
        "indonesian": "Indonesia",
        "israeli": "Israel",
        "italian": "Italy",
        "japanese": "Japan",
        "korean": "South Korea",
        "mexican": "Mexico",
        "nigerian": "Nigeria",
        "singaporean": "Singapore",
        "spanish": "Spain",
        "swiss": "Switzerland",
        "taiwanese": "Taiwan",
        "turkish": "Turkey",
        "vietnamese": "Vietnam",
    },
    "region": {
        "africa": "Africa",
        "asia": "Asia",
        "europe": "Europe",
        "european union": "European Union",
        "european": "Europe",
        "eu": "EU",
        "latin america": "Latin America",
        "middle east": "Middle East",
        "north america": "North America",
        "southeast asia": "Southeast Asia",
    },
    "macro_term": {
        "credit spreads": "credit spreads",
        "deposit beta": "deposit beta",
        "funding stress": "funding stress",
        "inflation": "inflation",
        "liquidity": "liquidity",
        "market depth": "market depth",
        "rates": "rates",
        "yield curve": "yield curve",
    },
    "software_product": {
        "airflow": "Airflow",
        "android": "Android",
        "anthropic api": "Anthropic API",
        "aws": "AWS",
        "azure": "Azure",
        "bloomberg terminal": "Bloomberg Terminal",
        "chatgpt": "ChatGPT",
        "claude": "Claude",
        "claude code": "Claude Code",
        "coinbase wallet": "Coinbase Wallet",
        "copilot": "Copilot",
        "cursor": "Cursor",
        "databricks": "Databricks",
        "datadog": "Datadog",
        "discord": "Discord",
        "docker": "Docker",
        "figma": "Figma",
        "fireblocks": "Fireblocks",
        "gemini": "Gemini",
        "github copilot": "GitHub Copilot",
        "github": "GitHub",
        "gitlab": "GitLab",
        "google cloud": "Google Cloud",
        "grafana": "Grafana",
        "gpt-4": "GPT-4",
        "gpt-4o": "GPT-4o",
        "gpt-5": "GPT-5",
        "jupyter": "Jupyter",
        "jira": "Jira",
        "kafka": "Kafka",
        "kubernetes": "Kubernetes",
        "ledger live": "Ledger Live",
        "linear": "Linear",
        "linux": "Linux",
        "llama": "Llama",
        "metamask": "MetaMask",
        "mongodb": "MongoDB",
        "netlify": "Netlify",
        "notion": "Notion",
        "openai api": "OpenAI API",
        "phantom wallet": "Phantom Wallet",
        "phantom": "Phantom",
        "postgres": "Postgres",
        "postgresql": "PostgreSQL",
        "python": "Python",
        "rabby": "Rabby",
        "redis": "Redis",
        "slack": "Slack",
        "snowflake": "Snowflake",
        "stripe radar": "Stripe Radar",
        "telegram": "Telegram",
        "terraform": "Terraform",
        "vscode": "VS Code",
        "vercel": "Vercel",
        "whatsapp": "WhatsApp",
        "windsurf": "Windsurf",
    },
    "coin": {
        "aave": "Aave",
        "atom": "Cosmos",
        "bera": "Berachain",
        "bnb": "BNB",
        "bonk": "Bonk",
        "chainlink": "Chainlink",
        "dot": "Polkadot",
        "dai": "DAI",
        "dogecoin": "Dogecoin",
        "ether": "Ether",
        "hedera": "Hedera",
        "hyperliquid": "Hyperliquid",
        "injective": "Injective",
        "jupiter": "Jupiter",
        "litecoin": "Litecoin",
        "monero": "Monero",
        "pepe": "Pepe",
        "polkadot": "Polkadot",
        "fdusd": "FDUSD",
        "paypal usd": "PayPal USD",
        "pyusd": "PYUSD",
        "rlusd": "RLUSD",
        "sei": "Sei",
        "shiba inu": "Shiba Inu",
        "stellar": "Stellar",
        "tether": "Tether",
        "usd coin": "USD Coin",
        "usdc": "USDC",
        "usdt": "USDT",
        "wrapped bitcoin": "Wrapped Bitcoin",
        "worldcoin": "Worldcoin",
        "xrp": "XRP",
    },
    "network": {
        "berachain": "Berachain",
        "arbitrum": "Arbitrum",
        "base": "Base",
        "bnb chain": "BNB Chain",
        "blast": "Blast",
        "celestia": "Celestia",
        "cosmos": "Cosmos",
        "ethereum mainnet": "Ethereum Mainnet",
        "hyperliquid": "Hyperliquid",
        "injective": "Injective",
        "mantle": "Mantle",
        "osmosis": "Osmosis",
        "optimism": "Optimism",
        "polygon": "Polygon",
        "sei": "Sei",
        "starknet": "Starknet",
        "world chain": "World Chain",
        "xrp ledger": "XRP Ledger",
        "zksync": "zkSync",
    },
    "amm": {
        "aerodrome": "Aerodrome",
        "balancer": "Balancer",
        "bancor": "Bancor",
        "camelot": "Camelot",
        "curve": "Curve",
        "maverick": "Maverick",
        "meteora": "Meteora",
        "orca": "Orca",
        "osmosis": "Osmosis",
        "pancakeswap": "PancakeSwap",
        "raydium": "Raydium",
        "sushiswap": "SushiSwap",
        "syncswap": "SyncSwap",
        "trader joe": "Trader Joe",
        "uniswap": "Uniswap",
        "velodrome": "Velodrome",
    },
}

AMBIGUOUS_ASSET_NETWORKS = {
    "aptos": ("Aptos", ("coin", "network")),
    "avalanche": ("Avalanche", ("coin", "network")),
    "bitcoin": ("Bitcoin", ("coin",)),
    "bnb": ("BNB", ("coin", "network")),
    "cardano": ("Cardano", ("coin", "network")),
    "ethereum": ("Ethereum", ("coin", "network")),
    "near": ("NEAR", ("coin", "network")),
    "polkadot": ("Polkadot", ("coin", "network")),
    "solana": ("Solana", ("coin", "network")),
    "sui": ("Sui", ("coin", "network")),
    "ton": ("TON", ("coin", "network")),
    "tron": ("Tron", ("coin", "network")),
}

COIN_CONTEXT_TERMS = {
    "asset",
    "buyback",
    "coin",
    "etf",
    "holders",
    "market cap",
    "price",
    "rallied",
    "rally",
    "selloff",
    "spot",
    "token",
    "traded",
    "trading",
    "treasury",
    "volume",
}

NETWORK_CONTEXT_TERMS = {
    "amm",
    "addresses",
    "bridge",
    "chain",
    "contract",
    "dex",
    "dapp",
    "developer",
    "ecosystem",
    "gas",
    "layer 1",
    "layer 2",
    "mainnet",
    "network",
    "protocol",
    "rollup",
    "testnet",
    "throughput",
    "validator",
}

AMM_DESCRIPTOR_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9.+-]*(?:\s+[A-Z][A-Za-z0-9.+-]*){0,2})\s+"
    r"(?:AMM|DEX|liquidity pool)\b"
)

SOFTWARE_DESCRIPTOR_PATTERN = re.compile(
    r"\b([A-Z][A-Za-z0-9.+-]*(?:\s+[A-Z][A-Za-z0-9.+-]*){0,2}\s+"
    r"(?:API|SDK|Wallet|Terminal|Cloud|Suite|Platform|App|Assistant|Copilot|"
    r"Model|Code|Browser|Radar|Studio|IDE|Agent))\b"
)
PERSON_PATTERNS = (
    re.compile(
        r"\b(?:CEO|CFO|COO|CTO|Chair|Chairman|Chairwoman|Founder|Co-Founder|"
        r"President|Prime Minister|Minister|Governor|Senator|Representative|"
        r"Secretary|Analyst|Investor|Chief Executive)\s+"
        r"([A-Z][a-z]+(?:\s+(?:[A-Z]\.|[A-Z][a-z]+)){1,2})\b"
    ),
    re.compile(
        r"\b([A-Z][a-z]+(?:\s+(?:[A-Z]\.|[A-Z][a-z]+)){1,2})\s+"
        r"(?:said|says|warned|wrote|announced|argued|noted|told|expects|"
        r"predicted|added|explained|called|stated)\b"
    ),
    re.compile(r"\b(?:Mr|Ms|Mrs|Dr)\.?\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b"),
    re.compile(
        r"\baccording to\s+([A-Z][a-z]+(?:\s+(?:[A-Z]\.|[A-Z][a-z]+)){1,2})\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b([A-Z][a-z]+(?:\s+(?:[A-Z]\.|[A-Z][a-z]+)){1,2}),\s+"
        r"(?:CEO|CFO|COO|CTO|Founder|Co-Founder|President|Chair)\b"
    ),
)
PERSON_STOPWORDS = {
    "africa",
    "april",
    "asia",
    "august",
    "business",
    "crypto",
    "europe",
    "fintech",
    "friday",
    "general",
    "january",
    "july",
    "june",
    "march",
    "markets",
    "monday",
    "november",
    "october",
    "policy",
    "read",
    "saturday",
    "september",
    "singapore",
    "sunday",
    "thursday",
    "tldr",
    "tuesday",
    "wednesday",
}


def _phrase_pattern(term: str) -> re.Pattern[str]:
    """Compile a boundary-aware matcher for a case-insensitive phrase."""

    return re.compile(rf"(?<!\w){re.escape(term)}(?!\w)", re.IGNORECASE)


GAZETTEER_PATTERNS = {
    entity_type: [
        (normalized, canonical, _phrase_pattern(normalized))
        for normalized, canonical in gazetteer.items()
    ]
    for entity_type, gazetteer in ENTITY_GAZETTEERS.items()
}

AMBIGUOUS_PATTERNS = {
    normalized: _phrase_pattern(normalized) for normalized in AMBIGUOUS_ASSET_NETWORKS
}

NON_PERSON_TERMS = (
    {normalized for gazetteer in ENTITY_GAZETTEERS.values() for normalized in gazetteer}
    | set(AMBIGUOUS_ASSET_NETWORKS)
    | {canonical.lower() for aliases in TICKER_ENTITY_ALIASES.values() for canonical, _ in aliases}
)


def _clean_entity_name(value: str) -> str:
    """Collapse repeated whitespace inside a matched entity string."""

    return " ".join(value.split())


def _valid_person_name(name: str) -> bool:
    """Reject obvious non-person title-case matches before storage."""

    normalized = name.lower()
    if normalized in NON_PERSON_TERMS:
        return False

    tokens = [token.rstrip(".").lower() for token in name.split()]
    if len(tokens) < 2:
        return False
    return not any(token in PERSON_STOPWORDS for token in tokens)


def _extract_people(text: str) -> Iterable[str]:
    """Find person names from lightweight title and attribution patterns."""

    for pattern in PERSON_PATTERNS:
        for match in pattern.finditer(text):
            name = _clean_entity_name(match.group(1))
            if _valid_person_name(name):
                yield name


def _extract_software_descriptors(text: str) -> Iterable[str]:
    """Find product-style names such as APIs, wallets, terminals, and apps."""

    for match in SOFTWARE_DESCRIPTOR_PATTERN.finditer(text):
        yield _clean_entity_name(match.group(1))


def _extract_amm_descriptors(text: str) -> Iterable[str]:
    """Find AMM and DEX-style names from title-cased descriptors in text."""

    for match in AMM_DESCRIPTOR_PATTERN.finditer(text):
        yield _clean_entity_name(match.group(1))


def _context_window(text: str, start: int, end: int, radius: int = 80) -> str:
    """Return a lowercase substring around a match for context scoring."""

    lower = text.lower()
    return lower[max(0, start - radius) : min(len(lower), end + radius)]


def _context_hits(window: str, vocabulary: set[str]) -> int:
    """Count how many context terms appear around a candidate entity mention."""

    return sum(term in window for term in vocabulary)


def _extract_ambiguous_assets_and_networks(
    text: str,
) -> Iterable[tuple[str, str, str, float]]:
    """Use nearby terms to classify ambiguous chain names as coins, networks, or both."""

    for normalized, (canonical, default_types) in AMBIGUOUS_ASSET_NETWORKS.items():
        coin_match = False
        network_match = False
        coin_context_match = False
        network_context_match = False
        saw_match = False
        for match in AMBIGUOUS_PATTERNS[normalized].finditer(text):
            saw_match = True
            window = _context_window(text, match.start(), match.end())
            coin_score = _context_hits(window, COIN_CONTEXT_TERMS)
            network_score = _context_hits(window, NETWORK_CONTEXT_TERMS)

            if coin_score > network_score and coin_score > 0:
                coin_match = True
                coin_context_match = True
            elif network_score > coin_score and network_score > 0:
                network_match = True
                network_context_match = True
            elif coin_score == network_score and coin_score > 0:
                coin_match = True
                network_match = True
                coin_context_match = True
                network_context_match = True
            else:
                if "coin" in default_types:
                    coin_match = True
                if "network" in default_types:
                    network_match = True

        if saw_match and "coin" in default_types and "network" in default_types:
            coin_match = True
            network_match = True

        if coin_match:
            confidence = 0.8 if normalized == "bitcoin" else (0.75 if coin_context_match else 0.6)
            yield canonical, "coin", normalized, confidence
        if network_match:
            confidence = 0.75 if network_context_match else 0.6
            yield canonical, "network", normalized, confidence


def _add_or_upgrade_entity(
    entities_by_key: dict[tuple[str, str], dict[str, str | float | None]],
    entity_text: str,
    entity_type: str,
    normalized: str | None = None,
    confidence: float = 0.7,
) -> None:
    """Add or upgrade one deduplicated entity record in the result mapping."""

    clean_text = _clean_entity_name(entity_text)
    normalized_value = (normalized or clean_text.lower()).lower()
    key = (entity_type, normalized_value)
    existing = entities_by_key.get(key)
    if existing is not None and float(existing["confidence"]) >= confidence:
        return
    entities_by_key[key] = {
        "entity_text": clean_text,
        "entity_type": entity_type,
        "normalized_value": normalized_value,
        "confidence": confidence,
    }


def _extract_heuristic_entities(text: str) -> list[dict[str, str | float | None]]:
    """Extract entities with the local gazetteer and rule-based heuristics."""

    entities_by_key: dict[tuple[str, str], dict[str, str | float | None]] = {}

    for match in ENTITY_PATTERNS["ticker"].finditer(text):
        token = match.group(0)
        normalized = token.replace("$", "").upper()
        if normalized in TICKER_STOPWORDS:
            continue
        confidence = 0.8 if token.startswith("$") else 0.65
        _add_or_upgrade_entity(entities_by_key, token, "ticker", normalized, confidence)
        for canonical, entity_type in TICKER_ENTITY_ALIASES.get(normalized, []):
            alias_confidence = 0.9 if token.startswith("$") else 0.82
            _add_or_upgrade_entity(
                entities_by_key,
                canonical,
                entity_type,
                canonical.lower(),
                alias_confidence,
            )

    for entity_type, patterns in GAZETTEER_PATTERNS.items():
        for normalized, canonical, pattern in patterns:
            if pattern.search(text):
                _add_or_upgrade_entity(entities_by_key, canonical, entity_type, normalized)

    for canonical, entity_type, normalized, confidence in _extract_ambiguous_assets_and_networks(
        text
    ):
        _add_or_upgrade_entity(entities_by_key, canonical, entity_type, normalized, confidence)

    for software_name in _extract_software_descriptors(text):
        _add_or_upgrade_entity(entities_by_key, software_name, "software_product", confidence=0.72)

    for amm_name in _extract_amm_descriptors(text):
        _add_or_upgrade_entity(entities_by_key, amm_name, "amm", confidence=0.73)

    for person_name in _extract_people(text):
        _add_or_upgrade_entity(entities_by_key, person_name, "person", confidence=0.78)

    return list(entities_by_key.values())


@lru_cache(maxsize=2)
def _load_ner_pipeline(model_name: str):
    """Load a transformers token-classification pipeline on demand."""

    try:
        from transformers import pipeline
    except ImportError as exc:  # pragma: no cover - optional dependency
        raise RuntimeError(
            "True NER requires the optional ml dependencies: pip install -e '.[ml]'"
        ) from exc
    return pipeline(
        "ner",
        model=model_name,
        aggregation_strategy="simple",
    )


def _ner_entity_type(label: str) -> str:
    """Map common NER label groups into the local entity taxonomy."""

    normalized = label.upper()
    if normalized in {"PER", "PERSON"}:
        return "person"
    if normalized in {"ORG", "ORGANIZATION"}:
        return "organization"
    if normalized in {"LOC", "LOCATION", "GPE"}:
        return "location"
    return "named_entity"


def _normalize_ner_entity(
    text: str,
    fallback_entity_type: str,
) -> list[tuple[str, str, str, float]]:
    """Map NER output into known local entity types when possible."""

    cleaned = _clean_entity_name(text)
    normalized = cleaned.lower()

    matches: list[tuple[str, str, str, float]] = []
    for entity_type, gazetteer in ENTITY_GAZETTEERS.items():
        canonical = gazetteer.get(normalized)
        if canonical is not None:
            matches.append((canonical, entity_type, normalized, 0.83))

    ambiguous = AMBIGUOUS_ASSET_NETWORKS.get(normalized)
    if ambiguous is not None:
        canonical, default_types = ambiguous
        for entity_type in default_types:
            matches.append((canonical, entity_type, normalized, 0.8))

    if matches:
        return matches

    if fallback_entity_type == "location":
        return [(cleaned, "location", normalized, 0.72)]
    if fallback_entity_type == "organization":
        return [(cleaned, "organization", normalized, 0.74)]
    if fallback_entity_type == "person":
        return [(cleaned, "person", normalized, 0.8)]
    return [(cleaned, fallback_entity_type, normalized, 0.68)]


def _extract_ner_entities(
    text: str,
    model_name: str,
) -> list[dict[str, str | float | None]]:
    """Extract entities with a model-backed NER pipeline."""

    entities_by_key: dict[tuple[str, str], dict[str, str | float | None]] = {}
    recognizer = _load_ner_pipeline(model_name)

    for item in recognizer(text):
        entity_text = str(item.get("word", "")).strip()
        if not entity_text:
            continue
        entity_type = _ner_entity_type(str(item.get("entity_group", "")))
        for canonical, normalized_type, normalized_value, confidence in _normalize_ner_entity(
            entity_text,
            entity_type,
        ):
            _add_or_upgrade_entity(
                entities_by_key,
                canonical,
                normalized_type,
                normalized_value,
                max(confidence, float(item.get("score", 0.0) or 0.0)),
            )

    return list(entities_by_key.values())


def extract_entities(
    text: str,
    *,
    backend: str = "heuristic",
    ner_model_name: str = "",
) -> list[dict[str, str | float | None]]:
    """Extract entities with heuristic, NER, or hybrid-NER backends."""

    if backend == "heuristic":
        return _extract_heuristic_entities(text)
    if backend == "ner":
        return _extract_ner_entities(text, ner_model_name)
    if backend == "hybrid-ner":
        entities_by_key: dict[tuple[str, str], dict[str, str | float | None]] = {}
        combined_entities = _extract_heuristic_entities(text) + _extract_ner_entities(
            text,
            ner_model_name,
        )
        for entity in combined_entities:
            _add_or_upgrade_entity(
                entities_by_key,
                str(entity["entity_text"]),
                str(entity["entity_type"]),
                str(entity.get("normalized_value")) if entity.get("normalized_value") else None,
                float(entity.get("confidence", 0.0) or 0.0),
            )
        return list(entities_by_key.values())
    msg = f"Unsupported entity extraction backend: {backend}"
    raise ValueError(msg)
