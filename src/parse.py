import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from dateutil import parser as dateparser

ROUND_UP = "ROUND_UP"
CASH_IN_LIEU = "CASH_IN_LIEU"
FRACTIONAL_ISSUED = "FRACTIONAL_ISSUED"
UNKNOWN = "UNKNOWN"

DATE_PATTERNS: List[re.Pattern] = [
    re.compile(r"effective\s+(?:as of\s+)?(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"will\s+become\s+effective[\w\s,]*?on\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"on\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s*,?\s*(?:at\s+\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm))?", re.IGNORECASE),
]

RATIO_PATTERN = re.compile(
    r"(?P<new>\d{1,3})\s*[- ]?for\s*[- ]?(?P<old>\d{1,3})", re.IGNORECASE
)


@dataclass
class Extraction:
    ratio_new: Optional[int]
    ratio_old: Optional[int]
    effective_date: Optional[datetime]
    rounding_policy: str
    matches_rounding: bool


def contains_reverse_split_language(text: str) -> bool:
    keywords = [
        "reverse stock split",
        "reverse split",
        "share consolidation",
    ]
    rounding = ["rounded up", "in lieu", "fractional"]
    text_lower = text.lower()
    return any(k in text_lower for k in keywords) and any(r in text_lower for r in rounding)


def extract_ratio(text: str) -> (Optional[int], Optional[int]):
    for match in RATIO_PATTERN.finditer(text):
        new = int(match.group("new"))
        old = int(match.group("old"))
        if new <= old:
            return new, old
    return None, None


def extract_effective_date(text: str, fallback: Optional[datetime]) -> Optional[datetime]:
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        date_str = match.group("date")
        try:
            dt = dateparser.parse(date_str, fuzzy=True)
            return dt
        except (ValueError, OverflowError):
            continue
    return fallback


def _contains_words(text: str, words) -> bool:
    text_lower = text.lower()
    return any(word in text_lower for word in words)


def classify_rounding_policy(text: str) -> str:
    text_lower = text.lower()
    if "rounded up" in text_lower or "rounding up" in text_lower:
        return ROUND_UP
    if "receive one" in text_lower and "in lieu" in text_lower:
        return ROUND_UP
    if "cash in lieu" in text_lower or "paid cash" in text_lower:
        return CASH_IN_LIEU
    if "no fractional shares" in text_lower and "issued" in text_lower:
        return FRACTIONAL_ISSUED
    return UNKNOWN


def extract_details(text: str, filed_at: datetime) -> Extraction:
    ratio_new, ratio_old = extract_ratio(text)
    effective = extract_effective_date(text, filed_at)
    rounding = classify_rounding_policy(text)
    return Extraction(
        ratio_new=ratio_new,
        ratio_old=ratio_old,
        effective_date=effective,
        rounding_policy=rounding,
        matches_rounding=rounding != UNKNOWN,
    )
