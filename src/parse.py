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

import re

def extract_reverse_split_context(text: str, window: int = 3500) -> str:
    t = text.lower()

    # High-signal anchors first
    strong = [
        "reverse stock split",
        "reverse split",
        "split-adjusted",
        "trading on a split-adjusted basis",
        "certificate of amendment",
    ]

    for k in strong:
        i = t.find(k)
        if i != -1:
            return text[max(0, i - window): i + window]

    # Weak anchors last (noisy; used only as fallback)
    weak = [
        "share consolidation",
        "stock consolidation",
    ]

    # If we must fall back, pick the best-looking weak hit (see Fix 2)
    candidates = []
    for k in weak:
        start = 0
        while True:
            i = t.find(k, start)
            if i == -1:
                break
            snippet = text[max(0, i - window): i + window]
            candidates.append(snippet)
            start = i + len(k)

    if not candidates:
        return ""

    # Score snippets: prefer ones that ALSO mention reverse-split-y words
    def score(sn: str) -> int:
        s = sn.lower()
        pos = 0
        pos += 5 if "reverse" in s else 0
        pos += 5 if "stock split" in s else 0
        pos += 3 if "effective" in s else 0
        pos += 3 if "split-adjusted" in s else 0
        pos += 2 if "common stock" in s else 0
        pos += 2 if "ratio" in s else 0
        # Penalize charter/conversion boilerplate
        neg = 0
        neg += 6 if "class a" in s and "class b" in s else 0
        neg += 4 if "conversion" in s else 0
        neg += 3 if "articles" in s else 0
        return pos - neg

    candidates.sort(key=score, reverse=True)
    return candidates[0]


def contains_reverse_split_language(text: str) -> bool:
    t = text.lower()
    strong = [
        "reverse stock split",
        "reverse split",
        "split-adjusted",
        "trading on a split-adjusted basis",
    ]
    return any(k in t for k in strong)



def _contains_words(text: str, words) -> bool:
    text_lower = text.lower()
    return any(word in text_lower for word in words)

def classify_rounding_policy(text: str) -> str:
    t = " ".join(text.lower().split())

    # Strong ROUND_UP (Beneficient-style)
    if (
        "additional share" in t
        and "fractional share" in t
        and "cash" not in t
    ):
        return ROUND_UP

    if "rounded up to the nearest whole share" in t:
        return ROUND_UP

    # Cash-in-lieu
    if "cash in lieu" in t or (
        "receive cash" in t and "fractional" in t
    ):
        return CASH_IN_LIEU

    return UNKNOWN


def extract_details(text: str, filed_at: datetime) -> Extraction:
    ctx = extract_reverse_split_context(text)
    ctx_l = ctx.lower()

    # If the "context" doesn't actually contain reverse split language, don't pretend it does
    if "reverse stock split" not in ctx_l and "reverse split" not in ctx_l:
        rounding = UNKNOWN
    else:
        rounding = classify_rounding_policy(ctx)

    ratio_new, ratio_old = extract_ratio(ctx)
    effective = extract_effective_date(ctx, filed_at)

    return Extraction(
        ratio_new=ratio_new,
        ratio_old=ratio_old,
        effective_date=effective,
        rounding_policy=rounding,
        matches_rounding=rounding != UNKNOWN,
    )


