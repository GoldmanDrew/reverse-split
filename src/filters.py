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


# NOTE:
# - Do NOT use broad keywords like "trust" against the full filing text.
#   "trust" shows up constantly (transfer agents, indenture trustees, stock transfer & trust companies)
#   and will create huge false exclusions (e.g., Beneficient).
ADR_KEYWORDS = ["adr", "american depositary", "american depository"]

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
CANADA_TITLE_KEYWORDS = ["canada", "ontario", "british columbia", "alberta", "quebec"]


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def is_adr(text: str, meta: SecurityInfo) -> bool:
    t = _norm(text)
    title = _norm(meta.title)
    # ADRs are usually clearly labeled; title signal is stronger than full-text
    if any(k in title for k in ADR_KEYWORDS):
        return True
    # Text can still be useful, but keep it simple
    return any(k in t for k in ADR_KEYWORDS)


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
    exch = (meta.exchange or "").strip().upper()
    country = (meta.country or "").strip().upper()
    title = _norm(meta.title)

    # Prefer metadata; do not exclude just because a filing text mentions TSX/Canada in passing.
    if country in {"CA", "CAN", "CANADA"}:
        return True
    if exch in CANADA_EXCHANGES:
        return True

    # Fallback: title indicates Canadian domicile (still much safer than scanning full text)
    return any(k in title for k in CANADA_TITLE_KEYWORDS)

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
