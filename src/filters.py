from dataclasses import dataclass
from typing import Optional

from .parse import CASH_IN_LIEU, FRACTIONAL_ISSUED, ROUND_UP


@dataclass
class SecurityInfo:
    ticker: str
    exchange: str
    title: str
    country: Optional[str] = None


ADR_KEYWORDS = ["adr", "american depositary", "american depository"]
ETF_KEYWORDS = ["etf", "exchange traded fund", "exchange-traded fund", "trust"]
CANADA_KEYWORDS = ["ontario", "british columbia", "tsx", "venture exchange", "canada"]


def is_adr(text: str, meta: SecurityInfo) -> bool:
    lower = text.lower()
    return any(word in lower for word in ADR_KEYWORDS) or "adr" in meta.title.lower()


def is_etf(text: str, meta: SecurityInfo) -> bool:
    lower = text.lower()
    return any(word in lower for word in ETF_KEYWORDS) or "etf" in meta.title.lower()


def is_canadian(text: str, meta: SecurityInfo) -> bool:
    lower = text.lower()
    return any(word in lower for word in CANADA_KEYWORDS)


def passes_security_filters(text: str, meta: SecurityInfo) -> bool:
    return not (is_adr(text, meta) or is_etf(text, meta) or is_canadian(text, meta))


def passes_rounding_policy(policy: str) -> bool:
    return policy == ROUND_UP


def passes_price_threshold(price: Optional[float], ratio_new: Optional[int], ratio_old: Optional[int]) -> bool:
    if price is None or not ratio_new or not ratio_old:
        return False
    multiplier = ratio_old / ratio_new
    return price * multiplier >= 1


def summarize_rejection(text: str, meta: SecurityInfo, policy: str, price: Optional[float], ratio_new: Optional[int], ratio_old: Optional[int]) -> Optional[str]:
    if not passes_security_filters(text, meta):
        return "Excluded security type (ADR/ETF/Canada)"
    if not passes_rounding_policy(policy):
        return "Rounding policy not round-up"
    if not passes_price_threshold(price, ratio_new, ratio_old):
        return "Fails price * ratio threshold"
    return None
