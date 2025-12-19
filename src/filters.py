# filters.py
from dataclasses import dataclass
from typing import Optional

from .parse import CASH_IN_LIEU, FRACTIONAL_ISSUED, ROUND_UP, UNKNOWN


@dataclass
class SecurityInfo:
    ticker: str
    exchange: str
    title: str
    country: Optional[str] = None


# Keep ETF detection tight and focused
ETF_TEXT_KEYWORDS = [
    "exchange-traded fund",
    "exchange traded fund",
    "etf",
    "open-end fund",
    "closed-end fund",
    "unit investment trust",
    "investment company act of 1940",
]

# Canada detection should primarily use metadata (exchange / country), not random text mentions.
CANADA_EXCHANGES = {"TSX", "TSXV", "CSE", "NEO", "CNQ"}  # common Canadian venues
CANADA_TITLE_EXPLICIT = [
    " inc. (canada)", # very explicit
    " corp. (canada)",
]

def _norm(s: str) -> str:
    return (s or "").strip().lower()


import re

# Strong, explicit ADR indicators (use regex with word boundaries)
_ADR_TITLE_RE = re.compile(
    r"\b(adr|ads)\b|american depositary|american depository|depositary (receipt|share)",
    re.I
)

# If you want a second-tier check, only look in the SEC header / document header area,
# not the entire filing body.
_HDR_SLICE_CHARS = 12000  # enough to capture <SEC-HEADER> and early cover page

def is_adr(text: str, meta: SecurityInfo) -> bool:
    """
    Return True only with strong evidence of ADR/ADS.

    Changes vs old:
      - Uses regex with WORD BOUNDARIES for 'ADR'/'ADS' (prevents substring matches).
      - Prefers title and header; avoids scanning entire filing body.
      - Defaults to False if uncertain.
    """
    title = _norm(meta.title)

    # 1) Title is the best signal (often literally includes 'ADR' / 'ADS')
    if title and _ADR_TITLE_RE.search(title):
        return True

    # 2) Header/cover-page only (NOT full text)
    #    Many false positives come from random body text; avoid that entirely.
    head = _norm((text or "")[:_HDR_SLICE_CHARS])
    if head and _ADR_TITLE_RE.search(head):
        # To reduce false positives further, require at least one strong phrase,
        # or ADR/ADS as standalone token near "depositary"
        hl = head.lower()
        if ("american depositary" in hl) or ("american depository" in hl) or ("depositary receipt" in hl) or ("depositary share" in hl):
            return True

        # If it is only 'adr'/'ads' without any depositary wording, treat as NOT ADR
        return False

    return False


def is_etf(text: str, meta: SecurityInfo) -> bool:
    title = _norm(meta.title)
    if " etf" in f" {title} " or title.endswith(" etf") or title.startswith("etf "):
        return True

    t = _norm(text)

    strong_signals = [
        "exchange-traded fund",
        "exchange traded fund",
        "open-end fund",
        "closed-end fund",
    ]
    weak_signals = [
        "investment company act of 1940",
        "unit investment trust",
    ]

    strong = any(s in t for s in strong_signals)
    weak = sum(1 for s in weak_signals if s in t)

    # require either a strong signal, or 2+ weak signals
    return strong or weak >= 2

def is_canadian(text: str, meta: SecurityInfo) -> bool:
    """
    Returns True ONLY when there is strong, explicit evidence the issuer is Canadian.
    Missing or ambiguous data defaults to False.
    """

    exch = (meta.exchange or "").strip().upper()
    country = (meta.country or "").strip().upper()
    title = _norm(meta.title)

    # 1) Strongest signal: explicit country metadata
    if country in {"CA", "CAN", "CANADA"}:
        return True

    # 2) Strong signal: Canadian exchange
    if exch in CANADA_EXCHANGES:
        return True

    # 3) Extremely conservative fallback: explicit issuer naming
    #    (Optional — you can delete this block entirely if you want zero risk)
    if title:
        for pat in CANADA_TITLE_EXPLICIT:
            if pat in title:
                return True

    # Default: NOT Canadian
    return False
NON_COMMON_SUFFIXES = ("W", "WS", "WT", "RT")

def is_non_common_security(meta: SecurityInfo) -> bool:
    t = (meta.ticker or "").upper()
    if not t:
        return True
    if t.endswith(NON_COMMON_SUFFIXES):
        return True
    if "^" in t or "/" in t or "-" in t:
        return True
    return False


def passes_security_filters(text: str, meta: SecurityInfo) -> bool:
    return not (is_adr(text, meta) or is_etf(text, meta) or is_canadian(text, meta))


def passes_rounding_policy(policy: str) -> bool:
    # Keep UNKNOWN allowed during tuning; tighten later if you want ROUND_UP only
    return policy in (ROUND_UP)


def passes_price_threshold(price: Optional[float], ratio_new: Optional[int], ratio_old: Optional[int]) -> bool:
    # If you aren't using price, callers should avoid invoking this (or guard it).
    if price is None or not ratio_new or not ratio_old:
        return False
    multiplier = ratio_old / ratio_new
    return price * multiplier >= 1

def summarize_rejection(
    text: str,
    meta: SecurityInfo,
    policy: str,
    price: Optional[float],
    ratio_new: Optional[int],
    ratio_old: Optional[int],
) -> Optional[str]:
    if not passes_security_filters(text, meta):
        return "Excluded security type (ADR/ETF/Canada)"

    if not passes_rounding_policy(policy):
        return "Rounding policy not allowed"

    # If you removed Yahoo pricing, do not block on price
    if price is not None:
        if not passes_price_threshold(price, ratio_new, ratio_old):
            return "Fails price * ratio threshold"
    
    if is_non_common_security(meta):
        return "Excluded non-common security (warrant/rights/unit/preferred)"

    return None
