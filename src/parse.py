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
    t = text.lower()
    keywords = ["reverse stock split", "reverse split", "share consolidation", "stock consolidation"]
    verbs = ["became effective", "will become effective", "effective on", "approved", "filed", "certificate of amendment"]

    return any(k in t for k in keywords) and any(v in t for v in verbs)


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
    t = text.lower()

    # Normalize whitespace to handle SEC .txt weird line breaks
    t_norm = re.sub(r"\s+", " ", t)

    # 1) Strong ROUND_UP: "additional share(s) ... in lieu of ... fractional share(s)"
    # Handles: "receive an additional share", "one additional share", "additional shares"
    if re.search(
        r"(receive|entitled to receive|shall receive).{0,120}additional\s+share(s)?"
        r".{0,180}in\s+lieu\s+of\s+(a\s+)?fractional\s+share(s)?",
        t_norm,
    ):
        return ROUND_UP

    # 2) Strong ROUND_UP: "rounded up to the nearest whole share"
    if re.search(r"rounded\s+up\s+to\s+the\s+nearest\s+whole\s+share", t_norm):
        return ROUND_UP

    # 3) Strong CASH_IN_LIEU: cash language near fractional language
    # (Important: require proximity so random "cash" elsewhere doesn't poison it.)
    if re.search(
        r"(cash\s+in\s+lieu|entitled to receive cash|will receive cash|paid\s+cash|cash\s+equal\s+to)"
        r".{0,250}(fractional|fraction\s+of\s+one\s+share)",
        t_norm,
    ) or re.search(
        r"(fractional|fraction\s+of\s+one\s+share).{0,250}"
        r"(cash\s+in\s+lieu|entitled to receive cash|will receive cash|paid\s+cash|cash\s+equal\s+to)",
        t_norm,
    ):
        return CASH_IN_LIEU

    # 4) Fractionals issued (rare)
    if re.search(r"fractional\s+share(s)?.{0,120}(will|shall)\s+be\s+(issued|distributed|delivered)", t_norm):
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
