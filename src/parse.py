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

RATIO_PAREN_PATTERN = re.compile(
    r"(?:one|\(?1\)?)\s*(?:share)?\s*(?:for|into|to)\s*(?:every\s*)?"
    r"(?:forty|\(?40\)?)\s*(?:shares?)",
    re.IGNORECASE
)

NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90,
}

PROSE_RATIO_PATTERN = re.compile(
    r"(?:every\s+)?(?P<old_word>[a-z]+)\s*\(?(?P<old_num>\d{1,3})\)?\s+shares?"
    r".{0,80}?"
    r"(?:one|\(?1\)?)\s+share",
    re.IGNORECASE
)

COLON_RATIO_PATTERN = re.compile(
    r"(?P<new>\d{1,3})\s*:\s*(?P<old>\d{1,3})",
    re.IGNORECASE
)


@dataclass
class Extraction:
    ratio_new: Optional[int]
    ratio_old: Optional[int]
    effective_date: Optional[datetime]
    rounding_policy: str
    matches_rounding: bool


import re
from typing import Optional, Tuple

# --- Patterns (broad but guarded by scoring) ---

RATIO_FOR_PATTERN = re.compile(
    r"(?P<new>\d{1,5})\s*(?:-?\s*for\s*-?)\s*(?P<old>\d{1,5})",
    re.IGNORECASE,
)

RATIO_COLON_PATTERN = re.compile(
    r"(?P<new>\d{1,5})\s*:\s*(?P<old>\d{1,5})",
    re.IGNORECASE,
)

RATIO_TO_PATTERN = re.compile(
    r"(?P<new>\d{1,5})\s*(?:-?\s*to\s*-?)\s*(?P<old>\d{1,5})",
    re.IGNORECASE,
)

# “every forty (40) shares ... one (1) share”
# Captures the numeric in parens; supports “each”, “every”, optional word-number.
PROSE_EVERY_PATTERN = re.compile(
    r"(?:each|every)\s+(?:[a-z\-]+\s*)?\(?(?P<old_num>\d{1,5})\)?\s+shares?"
    r".{0,120}?"
    r"(?:combined|converted|reclassified|changed|exchanged|reverse\s+split)\s+(?:into|to)\s+"
    r"(?:[a-z\-]+\s*)?\(?(?P<new_num>\d{1,5})\)?\s+shares?",
    re.IGNORECASE,
)

# Alternate prose: “(N) shares ... into one (1) share” without “each/every”
PROSE_INTO_PATTERN = re.compile(
    r"\(?(?P<old_num>\d{1,5})\)?\s+shares?"
    r".{0,140}?"
    r"(?:combined|converted|reclassified|changed|exchanged)\s+(?:into|to)\s+"
    r"\(?(?P<new_num>\d{1,5})\)?\s+shares?",
    re.IGNORECASE,
)

# Sometimes: “reverse split at a ratio of 1-for-40” or “on a 1:40 basis”
BASIS_HINT_PATTERN = re.compile(
    r"(?:ratio|basis)\b",
    re.IGNORECASE,
)


def extract_ratio(text: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Extract a reverse split ratio robustly across common SEC filing phrasings.

    Returns (ratio_new, ratio_old) where ratio is "new-for-old" (e.g., 1-for-40 => (1, 40)).
    """

    if not text:
        return None, None

    t = " ".join(text.split())
    tl = t.lower()

    # Anchor locations for scoring
    anchors = []
    for s in ("reverse stock split", "reverse split", "stock split"):
        idx = tl.find(s)
        if idx != -1:
            anchors.append(idx)
    anchor = min(anchors) if anchors else None

    def score_candidate(start: int, end: int, new: int, old: int) -> int:
        """
        Lower score = better candidate.
        We prefer:
          - proximity to "reverse split" anchor
          - nearby "ratio/basis", "effective", "will begin trading", "certificate of incorporation"
        We penalize:
          - "between", "range", "may", "up to", "from-to" language
        """
        if new <= 0 or old <= 0:
            return 10**9
        if new > old:  # not a reverse split style ratio
            return 10**9

        # Base distance to anchor (if any)
        dist = abs(start - anchor) if anchor is not None else 50_000

        window = tl[max(0, start - 180): min(len(tl), end + 180)]

        bonus = 0
        # Positive (reduce score)
        if BASIS_HINT_PATTERN.search(window):
            bonus -= 250
        if "effective" in window or "became effective" in window:
            bonus -= 200
        if "will begin trading" in window or "split-adjusted" in window:
            bonus -= 200
        if "certificate of incorporation" in window or "charter" in window:
            bonus -= 150
        if "board" in window or "approved" in window or "effected" in window:
            bonus -= 150

        # Negative (increase score)
        if "between" in window or "range" in window:
            bonus += 300
        if "may" in window or "up to" in window or "not more than" in window:
            bonus += 250
        if "to be determined" in window or "at the discretion" in window:
            bonus += 250

        # Mild preference for common reverse split sizes (doesn't overpower context)
        # (purely heuristic; keep it light)
        if new == 1 and old in {2, 3, 4, 5, 8, 10, 12, 15, 20, 25, 30, 40, 50, 60, 75, 80, 100, 200, 250, 500, 1000}:
            bonus -= 30

        return dist + bonus

    candidates = []

    # 1) Numeric X-for-Y
    for m in RATIO_FOR_PATTERN.finditer(t):
        new = int(m.group("new"))
        old = int(m.group("old"))
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old))

    # 2) Colon X:Y
    for m in RATIO_COLON_PATTERN.finditer(t):
        new = int(m.group("new"))
        old = int(m.group("old"))
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old))

    # 3) “X to Y” (rare but appears)
    for m in RATIO_TO_PATTERN.finditer(t):
        new = int(m.group("new"))
        old = int(m.group("old"))
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old))

    # 4) Prose “every (N) shares ... into (M) share”
    for m in PROSE_EVERY_PATTERN.finditer(t):
        old = int(m.group("old_num"))
        new = int(m.group("new_num"))
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old))

    for m in PROSE_INTO_PATTERN.finditer(t):
        old = int(m.group("old_num"))
        new = int(m.group("new_num"))
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old))

    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0])
    best_score, best_new, best_old = candidates[0]

    # Safety: sometimes filings list forward splits or other ratios; enforce reverse-split shape
    if best_new > best_old:
        return None, None

    return best_new, best_old


import re

_DELISTING_PATTERNS = [
    r"\bitem\s*3\.01\b",
    r"notice of delisting",
    r"nasdaq listing qualifications staff",
    r"deficiency letter",
    r"minimum (?:closing )?bid price of \$?1\.00",
    r"nasdaq listing rule\s*5550",
    r"nasdaq listing rule\s*5810",
    r"compliance period",
    r"subject to delisting",
]

_DELISTING_RE = re.compile("|".join(_DELISTING_PATTERNS), re.IGNORECASE)

def is_delisting_notice_only(text: str) -> bool:
    """
    Returns True if the filing looks like a Nasdaq deficiency/delisting notice
    without an executed reverse split (i.e., it's informational / compliance).
    """
    if not text:
        return False

    t = " ".join(text.split())
    if not _DELISTING_RE.search(t):
        return False

    # If it ALSO contains strong execution language, do NOT exclude.
    # (We only want to drop pure compliance notices.)
    strong_execution = [
        "we effected a reverse stock split",
        "the reverse stock split became effective",
        "effective as of",
        "will begin trading on a split-adjusted basis",
        "amendment to the certificate of incorporation was filed",
    ]
    tl = t.lower()
    if any(s in tl for s in strong_execution):
        return False

    return True


from datetime import datetime
import re
from typing import Optional
from dateutil import parser as dtparser
from datetime import datetime
from typing import Optional
import re
from dateutil import parser as dtparser

# Strong “winner” pattern: the date that is explicitly defined as the “Effective Time”
EFFECTIVE_TIME_PATTERN = re.compile(
    r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\s*"
    r"\(the\s+['\"]Effective\s+Time['\"]\)",
    re.IGNORECASE,
)

DATE_CANDIDATE_PATTERNS = [
    # will become effective ... on December 22, 2025
    re.compile(
        r"will\s+become\s+effective[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # effective as of 8:00 a.m. ... on December 22, 2025
    re.compile(
        r"effective\s+(?:as of\s+)?[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # begin trading ... on December 22, 2025
    re.compile(
        r"(?:begin|commence)\s+trading[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # generic MM/DD/YYYY
    re.compile(r"\bon\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\b", re.IGNORECASE),
]


def _score_date_context(ctx: str) -> int:
    """
    Lower is better.
    """
    c = ctx.lower()
    score = 0

    # Strong positives
    if "effective time" in c:
        score -= 1000  # make this overwhelmingly preferred
    if "will become effective" in c:
        score -= 700
    if "begin trading" in c or "commence trading" in c or "split-adjusted" in c:
        score -= 350

    # Strong negatives (common red herring: charter filing effective date)
    if "certificate of amendment" in c and "became effective" in c:
        score += 900
    if "secretary of state" in c:
        score += 400

    # Penalize “already effective” language vs future market-effective
    if "became effective" in c and "effective time" not in c:
        score += 250

    return score


def extract_effective_date(text: str) -> Optional[datetime]:
    """
    Extract the market effective date for the reverse split.

    Priority:
      1) Date explicitly defined as "(the 'Effective Time')"
      2) Best-scoring candidate from patterns ("will become effective", "begin trading", etc.)
      3) Tie-breaker: later date (helps when both charter-effective and market-effective appear)
    """
    if not text:
        return None

    t = " ".join(text.split())

    # 1) Hard-prefer explicit Effective Time definition
    m = EFFECTIVE_TIME_PATTERN.search(t)
    if m:
        try:
            return dtparser.parse(m.group("date"), fuzzy=True)
        except Exception:
            pass  # fall through to scored candidates

    # 2) Score all other candidates and pick the best
    candidates: list[tuple[int, datetime]] = []

    for pat in DATE_CANDIDATE_PATTERNS:
        for m in pat.finditer(t):
            raw = m.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            start = max(0, m.start() - 220)
            end = min(len(t), m.end() + 220)
            ctx = t[start:end]
            score = _score_date_context(ctx)

            candidates.append((score, dt))

    if not candidates:
        return None

    # Best score first; tie-breaker: later date wins
    candidates.sort(key=lambda x: (x[0], -x[1].timestamp()))
    return candidates[0][1]


import re

def extract_reverse_split_context(text: str, window: int = 5500) -> str:
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
    # normalize all whitespace/newlines to single spaces
    t = " ".join((text or "").lower().split())

    strong = [
        "reverse stock split",
        "reverse split",
        "split-adjusted",
        "trading on a split-adjusted basis",
        "reverse-stock split",   # optional: sometimes hyphenated
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
    
    if ("additional share" in t and "in lieu" in t and "fractional" in t) or \
    ("entitled to receive" in t and "additional" in t and "in lieu" in t and "fractional" in t):
        return ROUND_UP
    
    if "one whole share" in t and "fraction" in t and "in lieu" in t:
        return ROUND_UP

    # Cash-in-lieu
    if "cash in lieu" in t or (
        "receive cash" in t and "fractional" in t
    ):
        return CASH_IN_LIEU

    return UNKNOWN

from typing import Optional, Tuple

_TRADING_TABLE_ROW = re.compile(
    r"(Common Stock|Ordinary Shares|Class A Common Stock|Class B Common Stock).*?\b([A-Z]{1,6})\b.*?\b(NASDAQ|NYSE|AMEX|NYSE ARCA|NYSEARCA)\b",
    re.IGNORECASE | re.DOTALL,
)


def extract_common_ticker_exchange(text: str) -> Tuple[Optional[str], Optional[str]]:
    if not text:
        return None, None

    # compress whitespace to make tables searchable
    t = re.sub(r"\s+", " ", text)

    # Hard anchor on the 8-K trading-symbol table header language
    # (this appears in most 8-Ks when they list class / symbol / exchange)
    anchor = re.search(
        r"Title of each class.*?Trading Symbol.*?Name of each exchange",
        t,
        flags=re.IGNORECASE,
    )
    if not anchor:
        return None, None

    # Only search a limited window AFTER the header (avoid scanning whole doc)
    window = t[anchor.end(): anchor.end() + 2000]

    # Now look for a row that clearly refers to Common Stock / Ordinary Shares
    m = re.search(
        r"(Common Stock|Ordinary Shares|Class A Common Stock|Class B Common Stock)"
        r".{0,200}?\b([A-Z]{1,6})\b"
        r".{0,200}?\b(NASDAQ|NYSE|AMEX|NYSE ARCA|NYSEARCA)\b",
        window,
        flags=re.IGNORECASE,
    )
    if not m:
        return None, None

    ticker = m.group(2).upper()
    exch = m.group(3).upper().replace(" ", "")

    # Safety: reject obvious non-ticker captures
    if ticker in {"PAR", "VALUE", "SHARE", "STOCK"}:
        return None, None

    return ticker, exch



def extract_details(text: str, filed_at: datetime) -> Extraction:
    ctx = extract_reverse_split_context(text)
    ctx_l = ctx.lower()

    # If the "context" doesn't actually contain reverse split language, don't pretend it does
    if "reverse stock split" not in ctx_l and "reverse split" not in ctx_l:
        rounding = UNKNOWN
    else:
        rounding = classify_rounding_policy(ctx)

    ratio_new, ratio_old = extract_ratio(ctx)

    effective = extract_effective_date(ctx)

    return Extraction(
        ratio_new=ratio_new,
        ratio_old=ratio_old,
        effective_date=effective,
        rounding_policy=rounding,
        matches_rounding=rounding != UNKNOWN,
    )


