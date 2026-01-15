import re
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from dateutil import parser as dateparser
from zoneinfo import ZoneInfo
from typing import Optional, Tuple
import html

ROUND_UP = "ROUND_UP"
ROUND_DOWN = "ROUND_DOWN"          # NEW
CASH_IN_LIEU = "CASH_IN_LIEU"
FRACTIONAL_ISSUED = "FRACTIONAL_ISSUED"
UNKNOWN = "UNKNOWN"

DATE_PATTERNS: List[re.Pattern] = [
    # Existing
    re.compile(r"effective\s+(?:as of\s+)?(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"will\s+become\s+effective[\w\s,]*?on\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"on\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\s*,?\s*(?:at\s+\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm))?", re.IGNORECASE),

    # NEW: common alternatives
    re.compile(r"expects?\s+to\s+implement[\w\s,]*?effective\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"will\s+be\s+implemented[\w\s,]*?(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"will\s+(?:take\s+place|occur)\s+(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"(?:market\s+)?effective\s+date[\w\s,]*?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"begin\s+trading[\w\s,]*?(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
    re.compile(r"share\s+consolidation[\w\s,]*?(?:on\s+)?(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})", re.IGNORECASE),
]

# OCG-style: "receive one post-consolidation ... for every two hundred and twenty pre-consolidation ..."

PROSE_RECEIVE_FOR_EVERY_WORDS = re.compile(
    r"shareholders?\s+will\s+receive\s+(?:one|\(?1\)?)\s+"
    r"(?:post-?consolidation\s+)?(?:ordinary\s+)?shares?\s+for\s+every\s+"
    r"(?P<old_words>[a-z\s-]+?)\s+"
    r"(?:pre-?consolidation\s+)?(?:ordinary\s+)?shares?",
    re.IGNORECASE,
)

PROSE_RECEIVE_WORDS_PATTERN = re.compile(
    r"receive\s+(?P<new>(?:\d+|one|two|three|four|five|six|seven|eight|nine|ten))\s+"
    r"(?:post[-\s]*consolidation|post[-\s]*split|consolidated)?"
    r".{0,120}?\sfor\s+every\s+"
    r"(?P<old>(?:\d+|[a-z\s-]{3,120}?))\s+"
    r"(?=(?:pre[-\s]*consolidation|pre[-\s]*split|ordinary\s+shares|shares))",
    re.IGNORECASE | re.DOTALL
)


@dataclass
class Extraction:
    ratio_new: Optional[int]
    ratio_old: Optional[int]
    effective_date: Optional[datetime]
    rounding_policy: str
    matches_rounding: bool

# --- Patterns (broad but guarded by scoring) ---
NUM_COMMA = r"\d{1,3}(?:,\d{3})*|\d+"

RATIO_RANGE_PATTERN = re.compile(
    rf"ranging\s+from\s+(?P<a_new>{NUM_COMMA})\s*[-–]?\s*for\s*[-–]?\s*(?P<a_old>{NUM_COMMA})"
    rf"\s+to\s+(?P<b_new>{NUM_COMMA})\s*[-–]?\s*for\s*[-–]?\s*(?P<b_old>{NUM_COMMA})",
    re.IGNORECASE,
)

EXECUTION_WINDOW_RE = re.compile(
    r"(will\s+become\s+effective|became\s+effective|effective\s+on|effective\s+as\s+of|"
    r"will\s+begin\s+trading|begin\s+trading|split-adjusted|board\s+(?:has\s+)?determined|"
    r"board\s+(?:has\s+)?fixed|set\s+the\s+ratio|ratio\s+of\s+the\s+reverse\s+split)",
    re.IGNORECASE,
)


RATIO_FOR_PATTERN = re.compile(
    rf"\b(?P<new>{NUM_COMMA})\s*[-–]?\s*for\s*[-–]?\s*(?P<old>{NUM_COMMA})\b",
    re.IGNORECASE
)


RATIO_COLON_PATTERN = re.compile(
    r"(?P<new>\d{1,5})\s*:\s*(?P<old>\d{1,5})",
    re.IGNORECASE,
)

# “every forty (40) shares ... one (1) share”
# Captures the numeric in parens; supports “each”, “every”, optional word-number.
# “every ten (10) shares ... into one (1) share”
PROSE_EVERY_PATTERN = re.compile(
    r"(?:each|every)\s+(?:[a-z\-]+\s*)?\(?(?P<old_num>\d{1,5})\)?\s+shares?"
    r".{0,160}?"
    r"(?:automatically\s+)?(?:combined|converted|reclassified|changed|exchanged|reverse\s+split)\s+(?:into|to)\s+"
    r"(?:[a-z\-]+\s*)?\(?(?P<new_num>\d{1,5})\)?\s+shares?",
    re.IGNORECASE,
)

# Alternate prose: “(10) shares ... into one (1) share” without every/each
PROSE_INTO_PATTERN = re.compile(
    r"\(?(?P<old_num>\d{1,5})\)?\s+shares?"
    r".{0,180}?"
    r"(?:automatically\s+)?(?:combined|converted|reclassified|changed|exchanged|reverse\s+split)\s+(?:into|to)\s+"
    r"\(?(?P<new_num>\d{1,5})\)?\s+shares?",
    re.IGNORECASE,
)

PROSE_CONSOLIDATION_FOR_EVERY = re.compile(
    r"(?:shareholders?\s+will\s+)?receive\s+(?P<new>\d+|one|two|three|four|five|six|seven|eight|nine|ten)\s+"
    r".{0,80}?\sfor\s+every\s+(?P<old>\d+|[a-z\s-]{3,80})\s+"
    r"(?:pre[-\s]*consolidation|pre[-\s]*split|ordinary\s+shares|shares)",
    re.IGNORECASE | re.DOTALL
)

# Normalize common Unicode dashes to ASCII "-"
_DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
_DASH_RE = re.compile(f"[{_DASHES}]")

def _norm_text_html(s: str) -> str:
    """
    Normalizes SEC HTML-ish text so regexes can match reliably:
      - unescape HTML entities (&nbsp;, &#160;, etc.)
      - collapse NBSP to spaces
      - normalize unicode dashes/quotes
      - collapse whitespace
    """
    if not s:
        return ""
    s = html.unescape(s)              # <<< KEY for AMCOR (&nbsp; / &#160;)
    s = s.replace("\xa0", " ")        # NBSP
    s = _DASH_RE.sub("-", s)
    s = s.replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'")
    return " ".join(s.split())


def extract_ratio(text: str, debug_label: str = "") -> Tuple[Optional[int], Optional[int]]:
    """
    Returns (ratio_new, ratio_old) where ratio is "new-for-old".
    Example: 1-for-10 => (1, 10)
    """
    if not text:
        return None, None

    t = _norm_text_html(text)
    tl = t.lower()

        # Guardrail: authorization/range language (not a finalized split)
    if ("ratio of between" in tl or "within a range" in tl or "to be determined by our board" in tl) and \
       ("one-for-two" in tl and "one-for-ten" in tl) and \
       ("exact ratio" in tl or "to be determined" in tl or "in its discretion" in tl):
        # If there's no definitive numeric hyphenated X-for-Y, don't guess.
        if not re.search(r"\b\d{1,3}(?:,\d{3})*\s*-\s*for\s*-\s*\d{1,3}(?:,\d{3})*\b", tl):
            return None, None


    # Anchor locations for scoring (prefer ratios near reverse split discussion)
    anchors = []
    for s in ("reverse stock split", "reverse split", "split-adjusted"):
        idx = tl.find(s)
        if idx != -1:
            anchors.append(idx)
    anchor = min(anchors) if anchors else None

    def score_candidate(start: int, end: int, new: int, old: int) -> int:
        if new <= 0 or old <= 0:
            return 10**9
        if new >= old:  # <-- CHANGE: was only new > old
            return 10**9
        if new > old:
            return 10**9

        dist = abs(start - anchor) if anchor is not None else 50_000
        window = tl[max(0, start - 220): min(len(tl), end + 220)]

        bonus = 0
        # Strong context positives
        if "reverse stock split" in window or "reverse split" in window:
            bonus -= 400
        if "ratio" in window or "basis" in window:
            bonus -= 250
        if "effective time" in window or "will become effective" in window:
            bonus -= 250
        if "split-adjusted" in window or "will begin trading" in window:
            bonus -= 200

        # Negatives: “range / at discretion / up to” usually means not finalized
        if "between" in window or "range" in window:
            bonus += 350
        if "may" in window or "up to" in window or "not more than" in window:
            bonus += 300
        if "to be determined" in window or "at the discretion" in window:
            bonus += 300

        # Light heuristic preference for common reverse split ratios
        if new == 1 and old >= 2:
            bonus -= 25

        if new == 1 and old in {2,3,4,5,8,10,12,15,20,25,30,40,50,60,75,80,100,200,250,500,1000}:
            bonus -= 100

        return dist + bonus

    candidates = []

    def _to_int(x: str) -> int:
        return int(x.replace(",", "").strip())
    
    MONTH_WORDS = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "sept", "oct", "nov", "dec")
    def looks_like_date_nearby(start: int, end: int) -> bool:
        # Larger window so we can see reverse-split context even if a date is nearby
        w = tl[max(0, start - 80): min(len(tl), end + 120)]

        # If ratio is in actual reverse-split context, do NOT skip just because a date is nearby
        if any(k in w for k in (
            "reverse stock split",
            "reverse split",
            "split-adjusted",
            "will begin trading",
            "begin trading",
            "cusip",
            "effective at the opening of trading",
            "opening of trading",
        )):
            return False

        # Otherwise keep the conservative date filter
        if any(mo in w for mo in MONTH_WORDS) and re.search(r"\b(19|20)\d{2}\b", w) and "," in w:
            return True

        if "date of report" in w or "dated" in w:
            return True

        return False

    import re

    _NUMWORDS = {
        "zero":0,"one":1,"two":2,"three":3,"four":4,"five":5,"six":6,"seven":7,"eight":8,"nine":9,"ten":10,
        "eleven":11,"twelve":12,"thirteen":13,"fourteen":14,"fifteen":15,"sixteen":16,"seventeen":17,"eighteen":18,"nineteen":19,
        "twenty":20,"thirty":30,"forty":40,"fifty":50,"sixty":60,"seventy":70,"eighty":80,"ninety":90,
    }
    _SCALES = {"hundred":100, "thousand":1000}

    def words_to_int(s: str) -> int | None:
        s = s.lower()
        s = re.sub(r"[^a-z\s-]", " ", s)
        s = s.replace("-", " ")
        tokens = [t for t in s.split() if t not in ("and",)]
        if not tokens:
            return None

        total = 0
        current = 0
        seen = False

        for t in tokens:
            if t in _NUMWORDS:
                current += _NUMWORDS[t]
                seen = True
            elif t in _SCALES:
                seen = True
                scale = _SCALES[t]
                if current == 0:
                    current = 1
                current *= scale
                if scale >= 1000:
                    total += current
                    current = 0
            else:
                return None

        return total + current if seen else None


    def is_range_context(start: int, end: int) -> bool:
        w = tl[max(0, start - 200): min(len(tl), end + 200)]

        # Strong range indicators
        range_phrases = (
            "ranging from",
            "within a range",
            "ratio of between",
            "between",
            "range",
            "up to",
            "not more than",
            "to be determined",
            "in its sole discretion",
            "at the discretion",
        )

        if any(p in w for p in range_phrases):
            # Extra confirmation: often range language shows multiple ratios nearby (e.g. 1-for-10 to 1-for-30)
            if " to " in w and len(list(RATIO_FOR_PATTERN.finditer(w))) >= 2:
                return True
            # Or explicit “between one-for-x and one-for-y”
            if "between" in w and len(list(RATIO_FOR_PATTERN.finditer(w))) >= 2:
                return True
            # If it says "to be determined"/"discretion", it's range even with one ratio present
            if "to be determined" in w or "sole discretion" in w or "at the discretion" in w:
                return True

        # “aggregate ratio approved by shareholders” is usually authorization language
        if "aggregate ratio" in w and ("approved" in w) and ("shareholder" in w or "stockholder" in w):
            return True

        return False

    # --- Optional but very helpful: filter out CSS/HTML junk ratios in giant filings ---
    def looks_like_style_junk(start: int, end: int) -> bool:
        w = tl[max(0, start - 140): min(len(tl), end + 140)]
        junk_tokens = ("font:", "margin:", "style=", "<p", "</p", "<div", "</div", "&nbsp;", "times new roman")
        if any(tok in w for tok in junk_tokens):
            # Don't reject if it's clearly in reverse-split context
            if any(k in w for k in ("reverse stock split", "reverse split", "split-adjusted", "begin trading", "cusip", "effective")):
                return False
            return True
        return False

    # -----------------------------
    # 1) EXECUTION WINDOW: collect ALL candidates, choose BEST (do not early-return first match)
    # -----------------------------
    best_exec = None  # tuple: (score, new, old)

    for m in EXECUTION_WINDOW_RE.finditer(t):
        win_start = m.start()
        win_end = min(len(t), win_start + 2500)
        win = t[win_start:win_end]

        local_candidates = []

        # hyphenated X-for-Y
        for mm in RATIO_FOR_PATTERN.finditer(win):
            new = _to_int(mm.group("new"))
            old = _to_int(mm.group("old"))
            if not (0 < new < old):
                continue

            gs = win_start + mm.start()
            ge = win_start + mm.end()

            if looks_like_date_nearby(gs, ge):
                continue
            if is_range_context(gs, ge):
                continue
            if looks_like_style_junk(gs, ge):
                continue

            local_candidates.append((score_candidate(gs, ge, new, old), new, old))

        # colon X:Y
        for mm in RATIO_COLON_PATTERN.finditer(win):
            new = _to_int(mm.group("new"))
            old = _to_int(mm.group("old"))
            if not (0 < new < old):
                continue

            gs = win_start + mm.start()
            ge = win_start + mm.end()

            if looks_like_date_nearby(gs, ge):
                continue
            if is_range_context(gs, ge):
                continue
            if looks_like_style_junk(gs, ge):
                continue

            local_candidates.append((score_candidate(gs, ge, new, old), new, old))

        if local_candidates:
            local_candidates.sort(key=lambda x: x[0])
            cand = local_candidates[0]  # best in this window
            if best_exec is None or cand[0] < best_exec[0]:
                best_exec = cand

    # If we found something in execution window(s), return the best
    if best_exec is not None:
        _, new, old = best_exec
        return new, old

    # -----------------------------
    # 2) GLOBAL SCAN: add candidates (with junk/date/range filters)
    # -----------------------------
    for m in RATIO_FOR_PATTERN.finditer(t):
        new = _to_int(m.group("new"))
        old = _to_int(m.group("old"))
        if not (0 < new < old):
            continue

        if looks_like_date_nearby(m.start(), m.end()):
            continue
        if is_range_context(m.start(), m.end()):
            continue
        if looks_like_style_junk(m.start(), m.end()):
            continue

        window = tl[max(0, m.start() - 220): min(len(tl), m.end() + 220)]
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old, m.group(0), window))

    # Colon X:Y
    for m in RATIO_COLON_PATTERN.finditer(t):
        new = _to_int(m.group("new"))
        old = _to_int(m.group("old"))
        if not (0 < new < old):
            continue

        if looks_like_date_nearby(m.start(), m.end()):
            continue
        if is_range_context(m.start(), m.end()):
            continue
        if looks_like_style_junk(m.start(), m.end()):
            continue

        window = tl[max(0, m.start() - 220): min(len(tl), m.end() + 220)]
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old, m.group(0), window))

    if "shareholders will receive" in tl:
        i = tl.find("shareholders will receive")
        print("[DEBUG] receive-snippet:", tl[i:i+250])
        print("[DEBUG] prose matches:", len(list(PROSE_RECEIVE_WORDS_PATTERN.finditer(tl))))


    # Prose: each/every ... (N) shares ... into (M) shares
    for m in PROSE_EVERY_PATTERN.finditer(t):
        old = _to_int(m.group("old_num"))
        new = _to_int(m.group("new_num"))
        if not (0 < new < old):
            continue

        if looks_like_date_nearby(m.start(), m.end()):
            continue
        if looks_like_style_junk(m.start(), m.end()):
            continue

        window = tl[max(0, m.start() - 220): min(len(tl), m.end() + 220)]
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old, m.group(0), window))

    # Prose alternate: (N) shares ... into (M) shares
    for m in PROSE_INTO_PATTERN.finditer(t):
        old = _to_int(m.group("old_num"))
        new = _to_int(m.group("new_num"))
        if not (0 < new < old):
            continue

        if looks_like_date_nearby(m.start(), m.end()):
            continue
        if looks_like_style_junk(m.start(), m.end()):
            continue

        window = tl[max(0, m.start() - 220): min(len(tl), m.end() + 220)]
        candidates.append((score_candidate(m.start(), m.end(), new, old), new, old, m.group(0), window))

    for m in PROSE_RECEIVE_WORDS_PATTERN.finditer(tl):
        new_raw = m.group("new").strip()
        old_raw = m.group("old").strip()

        # parse new
        new = int(new_raw) if new_raw.isdigit() else words_to_int(new_raw)
        # parse old (likely words)
        old = int(old_raw) if old_raw.strip().isdigit() else words_to_int(old_raw)

        if new is None or old is None:
            continue
        if not (0 < new < old):
            continue

        start, end = m.start(), m.end()

        window = tl[max(0, start-220): min(len(tl), end+220)]
        candidates.append((score_candidate(start, end, new, old), new, old, m.group(0), window))

    for m in PROSE_CONSOLIDATION_FOR_EVERY.finditer(tl):
        new_raw = m.group("new").strip()
        old_raw = m.group("old").strip()

        new = int(new_raw) if new_raw.isdigit() else words_to_int(new_raw)
        old = int(old_raw) if old_raw.isdigit() else words_to_int(old_raw)

        if new is None or old is None:
            continue
        if not (0 < new < old):
            continue

        start, end = m.start(), m.end()
        if looks_like_style_junk(start, end):
            continue

        window = tl[max(0, start-220): min(len(tl), end+220)]
        candidates.append((score_candidate(start, end, new, old), new, old, m.group(0), window))

    # Authorization-style range (fallback only)
    rm = RATIO_RANGE_PATTERN.search(t)
    if rm:
        a_new, a_old = _to_int(rm.group("a_new")), _to_int(rm.group("a_old"))
        b_new, b_old = _to_int(rm.group("b_new")), _to_int(rm.group("b_old"))

        lo = (a_new, a_old) if a_old <= b_old else (b_new, b_old)
        hi = (b_new, b_old) if a_old <= b_old else (a_new, a_old)

        if "will become effective" in tl or "begin trading on a split-adjusted basis" in tl:
            return hi


    if not candidates:
        return None, None

    candidates.sort(key=lambda x: x[0])

    _, best_new, best_old, _, _ = candidates[0]
    return best_new, best_old

# --- NEW: header event-date parser (Period of Report / earliest event date) ---
# --- Event date extractor (Period of Report / earliest event reported) ---

_HDR_SLICE_CHARS_EVENTDATE = 40000  # cover SEC-HEADER + front page

_CONFORMED_PERIOD_RE = re.compile(
    r"(?:CONFORMED\s+)?PERIOD\s+OF\s+REPORT:\s*(\d{8})",
    re.IGNORECASE,
)

_DATE_OF_REPORT_RE = re.compile(
    r"DATE\s+OF\s+REPORT\s*\(Date\s+of\s+earliest\s+event\s+reported\)\s*:\s*([A-Za-z]{3,9}\s+\d{1,2},\s+\d{4})",
    re.IGNORECASE,
)

def extract_event_reported_datetime(
    text: str,
    tz: ZoneInfo = ZoneInfo("America/New_York"),
) -> Optional[datetime]:
    """
    Returns tz-aware datetime at 00:00 local time for the event date:
      1) (CONFORMED) PERIOD OF REPORT: YYYYMMDD   (most reliable)
      2) DATE OF REPORT (Date of earliest event reported): Month DD, YYYY
    """
    if not text:
        return None

    head = text[:_HDR_SLICE_CHARS_EVENTDATE]

    m = _CONFORMED_PERIOD_RE.search(head)
    if m:
        d = datetime.strptime(m.group(1), "%Y%m%d")
        return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)

    m = _DATE_OF_REPORT_RE.search(head)
    if m:
        try:
            d = dtparser.parse(m.group(1), fuzzy=True)
            return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=tz)
        except Exception:
            return None

    return None


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

from typing import Optional
from dateutil import parser as dtparser
import re
from datetime import datetime
# --- replace your effective-date patterns + extract_effective_date with this ---

# (the “Effective Time”) variants show up a lot
EFFECTIVE_TIME_DEF_PATTERN = re.compile(
    r"\b(?:as\s+of\s+)?(?P<time>\d{1,2}:\d{2}\s*(?:a\.m\.|p\.m\.|am|pm)\s*(?:eastern|et|e\.t\.)?)"
    r"[\w\s,;:\-()]{0,60}?\bon\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})"
    r"[\w\s,;:\-()]{0,60}?\(\s*the\s+['\"\u201c\u201d]?\s*effective\s+time\s*['\"\u201c\u201d]?\s*\)",
    re.IGNORECASE,
)

# Also sometimes: "December 15, 2025 (the “Effective Time”)"
EFFECTIVE_TIME_PAREN_PATTERN = re.compile(
    r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})\s*"
    r"\(\s*the\s+['\"\u201c\u201d]?\s*effective\s+time\s*['\"\u201c\u201d]?\s*\)",
    re.IGNORECASE,
)

# AMCOR-style: "will proceed with the reverse stock split on January 14, 2026"
PROCEED_RSS_PATTERN = re.compile(
    r"\b(?:will\s+)?(?:proceed|proceeds|proceeded)\s+with\s+the\s+reverse\s+(?:stock\s+)?split"
    r"[\w\s,:;()\-]{0,80}?\bon\s+(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)

# Generic: "... reverse stock split on January 14, 2026"
REVERSE_SPLIT_ON_PATTERN = re.compile(
    r"\breverse\s+(?:stock\s+)?split\b[\w\s,:;()\-]{0,80}?\bon\s+"
    r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)

DATE_CANDIDATE_PATTERNS = [
    PROCEED_RSS_PATTERN,
    REVERSE_SPLIT_ON_PATTERN,

    re.compile(
        r"expects?\s+that[\w\s,:;()\-]*?\bwill\s+begin\s+trading[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    re.compile(
        r"will\s+be\s+reflected\s+in\s+the\s+trading[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),

    # will become effective ... on January 14, 2026
    re.compile(
        r"\bwill\s+(?:become|be)\s+effective[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # became effective ... on January 14, 2026
    re.compile(
        r"\bbecame\s+effective[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    # begin trading / split-adjusted trading on January 15, 2026
    re.compile(
        r"\b(?:begin|began|commence|commencing|will\s+begin)\s+trading[\w\s,:;()\-]*?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    re.compile(
        r"\bsplit-?adjusted\b[\w\s,:;()\-]{0,80}?\bon\s+"
        r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
        re.IGNORECASE,
    ),
    re.compile(r"\bon\s+(?P<date>\d{1,2}/\d{1,2}/\d{2,4})\b", re.IGNORECASE),
]

MONTH_DATE_ANYWHERE = re.compile(
    r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})",
    re.IGNORECASE,
)

NUM_DATE_ANYWHERE = re.compile(
    r"(?P<date>\d{1,2}/\d{1,2}/\d{2,4})",
    re.IGNORECASE,
)

# Phrases that usually precede the MARKET effective date
EFFECTIVE_TRIGGERS = [
    # highest signal
    (re.compile(r"\beffective time\b", re.IGNORECASE), -2500),
    (re.compile(r"\bwill become effective\b", re.IGNORECASE), -1600),
    (re.compile(r"\bbecome effective\b", re.IGNORECASE), -1200),
    (re.compile(r"\bwill be effective\b", re.IGNORECASE), -1200),
    (re.compile(r"\beffective as of\b", re.IGNORECASE), -900),

    # market/trading signal (your PRPH 12/22 case)
    (re.compile(r"\breflected in the trading\b", re.IGNORECASE), -1500),
    (re.compile(r"\btrading on a split-adjusted basis\b", re.IGNORECASE), -1500),
    (re.compile(r"\bsplit-adjusted basis\b", re.IGNORECASE), -1300),
    (re.compile(r"\bwill begin trading\b", re.IGNORECASE), -1200),
    (re.compile(r"\bbegin trading\b", re.IGNORECASE), -900),
    (re.compile(r"\bcommence trading\b", re.IGNORECASE), -900),
]

# Phrases that often indicate NON-market dates (approval, charter filing)
NON_EFFECTIVE_NEGATIVES = [
    re.compile(r"\bstockholders approved\b", re.IGNORECASE),
    re.compile(r"\bshareholders approved\b", re.IGNORECASE),
    re.compile(r"\bannual meeting\b", re.IGNORECASE),
    re.compile(r"\bspecial meeting\b", re.IGNORECASE),
    re.compile(r"\bapproved an amendment\b", re.IGNORECASE),
    re.compile(r"\bstate of delaware\b", re.IGNORECASE),
    re.compile(r"\bsecretary of state\b", re.IGNORECASE),
    re.compile(r"\bcertificate of amendment\b", re.IGNORECASE),
]

def _find_dates_near_triggers(text: str, filed_at: Optional[datetime] = None) -> list[tuple[int, datetime, str]]:
    """
    Returns list of (score, date, ctx) candidates found by scanning for trigger phrases
    and grabbing the nearest date after each trigger within a window.
    """
    if not text:
        return []

    t = _norm_text_html(text)
    out: list[tuple[int, datetime, str]] = []

    for trig_re, trig_bonus in EFFECTIVE_TRIGGERS:
        for m in trig_re.finditer(t):
            # look ahead ~400 chars for a date
            start = m.start()
            end = min(len(t), m.end() + 450)
            window = t[start:end]

            # find the first plausible date in the window (month-name preferred)
            dm = MONTH_DATE_ANYWHERE.search(window) or NUM_DATE_ANYWHERE.search(window)
            if not dm:
                continue

            raw = dm.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            ctx = window[max(0, dm.start()-200): min(len(window), dm.end()+200)]
            score = trig_bonus + _score_date_context(ctx)

            # filing-date sanity: market effective date is usually on/after filing,
            # or very close to filing. Penalize older dates HARD unless context is strong.
            if filed_at is not None:
                days_diff = (dt.date() - filed_at.date()).days
                if days_diff < -2:
                    score += 1200
                elif days_diff < 0:
                    score += 500

            out.append((score, dt, ctx))

    return out

def _score_date_context(ctx: str) -> int:
    c = ctx.lower()
    score = 0

    # positives (keep whatever you have)
    if "effective time" in c:
        score -= 2000
    if "reflected in the trading" in c:
        score -= 1500
    if "split-adjusted" in c:
        score -= 1300
    if "will begin trading" in c or "begin trading" in c or "commence trading" in c:
        score -= 1000
    if "will become effective" in c or "become effective" in c or "will be effective" in c or "begin trading on a split-adjusted basis" in c:
        score -= 2200

    if "certificate of amendment" in c:
        score += 1500
    if "secretary of state" in c:
        score += 1500
    if "state of delaware" in c:
        score += 1800

    # NEW: if "effective time" appears with charter filing language, treat it as NOT the market date
    if "effective time" in c and (
        "certificate of amendment" in c
        or "secretary of state" in c
        or "state of delaware" in c
    ):
        score += 3000  # overpower the effective-time bonus

    return score

IMPLEMENTED_EFFECTIVE_RE = re.compile(
    r"\bimplemented\s+effective\s+([A-Za-z]+\s+\d{1,2},\s+\d{4})\b",
    re.IGNORECASE
)


from datetime import datetime, timedelta
from typing import Optional


def extract_effective_date(text: str, filed_at: Optional[datetime] = None) -> Optional[datetime]:
    """
    Extract the reverse-split MARKET effective / trading date.

    Key rules:
      - Bucket candidates into MARKET vs NON-MARKET; if any MARKET candidates exist, only pick among them.
      - Prefer dates on/after filing date (with a small negative buffer), but do NOT hard-require it
        because some 8-Ks are filed after the split is already effective.
      - If only non-market candidates exist and they look like approval/Delaware/charter dates,
        return None (avoid false positives).
    """
    if not text:
        return None

    t = _norm_text_html(text)

    # 0) explicit Effective Time definition (highest precision)
    m = EFFECTIVE_TIME_DEF_PATTERN.search(t)
    if m:
        try:
            return dtparser.parse(m.group("date"), fuzzy=True)
        except Exception:
            pass

    market_candidates: list[tuple[int, datetime, str]] = []
    other_candidates: list[tuple[int, datetime, str]] = []

    def _is_charter_ctx(cl: str) -> bool:
        return (
            "certificate of amendment" in cl
            or "secretary of state" in cl
            or "state of delaware" in cl
        )

    def _is_market_ctx(cl: str) -> bool:
        if _is_charter_ctx(cl):
            return (
                "reflected in the trading" in cl
                or "split-adjusted" in cl
                or "begin trading" in cl
                or "begin to trade" in cl          # NEW
                or "begins to trade" in cl         # NEW
                or "commence trading" in cl
                or "trade on a split-adjusted basis" in cl
                or "market open" in cl
            )

        return (
            "reflected in the trading" in cl
            or "split-adjusted" in cl
            or "begin trading" in cl
            or "begin to trade" in cl              # NEW
            or "begins to trade" in cl             # NEW
            or "commence trading" in cl
            or "trade on a split-adjusted basis" in cl
            or "market open" in cl
            or "effective time" in cl
            or "will become effective" in cl
            or "become effective" in cl
            or "implemented effective" in cl
            or "market effective date" in cl
        )


    # --- NEW: direct “implemented effective <date>” candidates (treat as MARKET) ---
    IMPLEMENTED_EFFECTIVE_PATTERNS = [
        re.compile(r"\bimplemented\s+effective\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE),
        re.compile(r"\bimplemented\s+on\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE),
        re.compile(r"\bmarket\s+effective\s+date\b.*?(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE | re.DOTALL),

        # NEW: market trading phrasing (covers your edge case)
        re.compile(r"\bbegin(?:s)?\s+to\s+trade\b.*?\bon\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b",
                re.IGNORECASE | re.DOTALL),
    ]

    for pat in IMPLEMENTED_EFFECTIVE_PATTERNS:
        for m in pat.finditer(t):
            raw = m.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            # give it a very good score; it is explicitly "effective"
            market_candidates.append((-2000, dt, t[max(0, m.start()-120): min(len(t), m.end()+120)]))

    # 1) Trigger-based candidates
    trig_cands = _find_dates_near_triggers(t, filed_at=filed_at)

    for item in trig_cands:
        if len(item) == 4:
            score, dt, ctx, kind = item
        else:
            score, dt, ctx = item
            kind = "market" if _is_market_ctx(ctx.lower()) else "non_market"

        if kind == "market":
            market_candidates.append((score, dt, ctx))
        else:
            other_candidates.append((score, dt, ctx))

    # 2) Pattern-based candidates
    for pat in DATE_CANDIDATE_PATTERNS:
        for m in pat.finditer(t):
            raw = m.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            start = max(0, m.start() - 260)
            end = min(len(t), m.end() + 260)
            ctx = t[start:end]
            score = _score_date_context(ctx)

            # filed_at sanity (light)
            if filed_at is not None:
                days_diff = (dt.date() - filed_at.date()).days
                if days_diff < -2:
                    score += 1200
                elif days_diff < 0:
                    score += 500

            if _is_market_ctx(ctx.lower()):
                market_candidates.append((score, dt, ctx))
            else:
                other_candidates.append((score, dt, ctx))

    def _choose_best(cands: list[tuple[int, datetime, str]]) -> datetime:
        cands_sorted = sorted(cands, key=lambda x: (x[0], -x[1].timestamp()))

        if filed_at is None:
            return cands_sorted[0][1]

        threshold = (filed_at - timedelta(days=1)).date()

        for score, dt, ctx in cands_sorted:
            if dt.date() >= threshold:
                return dt

        return cands_sorted[0][1]

    # 3) Selection rule:
    if market_candidates:
        return _choose_best(market_candidates)

    if not other_candidates:
        return None

    best_dt = _choose_best(other_candidates)

    best_ctx = min(other_candidates, key=lambda x: (x[0], -x[1].timestamp()))[2]
    lower = best_ctx.lower()

    looks_non_market = any(rx.search(lower) for rx in NON_EFFECTIVE_NEGATIVES)
    saw_market_anywhere = any(trig.search(t.lower()) for trig, _ in EFFECTIVE_TRIGGERS)

    if looks_non_market and not saw_market_anywhere:
        return None

    return best_dt

_DASHES = "\u2010\u2011\u2012\u2013\u2014\u2015\u2212"
_DASH_RE = re.compile(f"[{_DASHES}]")

def _norm_text_basics(s: str) -> str:
    if not s:
        return ""
    s = _DASH_RE.sub("-", s)
    s = s.replace("“", '"').replace("”", '"').replace("’", "'").replace("‘", "'")
    return " ".join(s.split())

def extract_reverse_split_context(text: str, window: int = 6500) -> str:
    if not text:
        return ""

    t = _norm_text_html(text)
    tl = t.lower()

    anchors = [
        "effective time",
        "will become effective",
        "begin trading",
        "commence trading",
        "split-adjusted",
        "trading on a split-adjusted basis",
        "reverse stock split",
        "reverse split",
        # keep it, but penalize it (often points to the wrong “became effective” clause)
        "share consolidation",
        "stock consolidation",
        "post-consolidation",
        "pre-consolidation",
        "no fractional shares",
        "fractional shares will be",
    ]

    candidates: List[Tuple[int, str]] = []
    seen = set()

    def add(i: int):
        lo = max(0, i - window)
        hi = min(len(t), i + window)
        key = (lo // 300, hi // 300)
        if key in seen:
            return
        seen.add(key)
        candidates.append((i, t[lo:hi]))

    for k in anchors:
        start = 0
        hits = 0
        while True:
            i = tl.find(k, start)
            if i == -1:
                break
            add(i)
            hits += 1
            start = i + len(k)
            if hits >= 6:
                break
        if len(candidates) >= 30:
            break

    if not candidates:
        return t[:9000]

    def score(sn: str) -> int:
        s = sn.lower()
        sc = 0
        if "effective time" in s: sc += 200
        if "will become effective" in s: sc += 160
        if "split-adjusted" in s or "begin trading" in s or "commence trading" in s: sc += 120
        if "reverse stock split" in s: sc += 80
        if "reverse split" in s: sc += 50
        if "fractional" in s: sc += 30
        if "rounded up" in s or "round up" in s: sc += 30
        if re.search(r"\b\d{1,4}\s*-\s*for\s*-\s*\d{1,4}\b", s) or re.search(r"\b\d{1,4}\s*for\s*\d{1,4}\b", s):
            sc += 40

        # penalize the common trap that caused PRPH
        if "certificate of amendment" in s and "became effective" in s:
            sc -= 250
        if "secretary of state" in s:
            sc -= 80

        return sc

    candidates.sort(key=lambda x: (score(x[1]), x[0]), reverse=True)
    return candidates[0][1]


def contains_reverse_split_language(text: str) -> bool:
    t = " ".join((text or "").lower().split())

    strong = [
        "reverse stock split",
        "reverse split",
        "split-adjusted",
        "trading on a split-adjusted basis",
        "reverse-stock split",
        # NEW (OCG / FPIs):
        "share consolidation",
        "stock consolidation",
        "consolidation of shares",
        "post-consolidation",
        "pre-consolidation",
    ]

    if any(k in t for k in strong):
        # Guard: avoid matching generic “consolidated financial statements”
        if "consolidated financial statements" in t and (
            "share consolidation" not in t and "stock consolidation" not in t
        ):
            return False
        return True

    return False



def _contains_words(text: str, words) -> bool:
    text_lower = text.lower()
    return any(word in text_lower for word in words)

def classify_rounding_policy(text: str) -> str:
    t = " ".join((text or "").lower().split())

    # ----------------------------
    # 1) ROUND_UP (maximize recall)
    # ----------------------------
    # If the document is presenting alternatives, do NOT classify as round-up.
    # Example: "... pay in cash ... OR ... rounded up ..."
    if ("pay in cash" in t or "cash" in t) and "rounded up" in t and " or " in t:
        return UNKNOWN

        # ----------------------------
    # 0) ROUND_DOWN (explicit reject case)
    # ----------------------------
    if "rounded down" in t and ("fractional" in t or "fraction" in t):
        return ROUND_DOWN
    if "round down" in t and ("fractional" in t or "fraction" in t):
        return ROUND_DOWN


    # A) "rounded up" family (covers CODX: "rounded up to the next whole number")
    if "rounded up" in t and ("fractional" in t or "no fractional" in t):
        # Many filings say "rounded up to the nearest/next whole share/number"
        if ("whole share" in t) or ("whole number" in t) or ("nearest whole" in t) or ("next whole" in t):
            return ROUND_UP
        # Even if they don't say whole share/number explicitly, "rounded up" + fractional is usually enough
        return ROUND_UP

    # B) Exact common phrases (keep your existing one + broaden it)
    if "rounded up to the nearest whole share" in t:
        return ROUND_UP
    if "rounded up to the nearest whole number" in t:
        return ROUND_UP
    if "rounded up to the next whole share" in t:
        return ROUND_UP
    if "rounded up to the next whole number" in t:
        return ROUND_UP

    # C) "No fractional shares will be issued" + rounding wording
    if ("no fractional shares" in t or "no fractional share" in t) and ("round" in t and "up" in t):
        return ROUND_UP

    # D) "one (1) whole share" / "additional share" in lieu of fractional
    # (broader than your current checks)
    if ("one whole share" in t or "one (1) whole share" in t) and ("fractional" in t or "fraction" in t) and ("in lieu" in t):
        return ROUND_UP

    if ("additional share" in t) and ("fractional" in t or "fraction" in t) and ("in lieu" in t):
        return ROUND_UP

    if ("entitled to receive" in t) and ("additional" in t) and ("in lieu" in t) and ("fractional" in t or "fraction" in t):
        return ROUND_UP

    # E) Another very common legal phrasing:
    # "any holder otherwise entitled to a fractional share shall receive one whole share"
    if ("otherwise entitled" in t) and ("fractional" in t) and ("shall receive" in t) and ("whole share" in t):
        return ROUND_UP

    # ----------------------------
    # 2) CASH IN LIEU (more specific)
    # ----------------------------
    # We only classify cash-in-lieu when cash is clearly tied to fractional shares.
    if "cash in lieu" in t and ("fractional" in t or "fraction" in t):
        return CASH_IN_LIEU

    if ("paid in cash" in t or "receive cash" in t) and ("fractional" in t or "fraction" in t):
        return CASH_IN_LIEU

    if ("cash payment" in t or "cash payments" in t) and ("fractional" in t or "fraction" in t):
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

import re
from dateutil import parser as dtparser

# Month-name date, tolerate extra spaces and optional comma
DATE_ANY = re.compile(r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})")

# HIGH-signal market/trading triggers (these should correlate with the *market* effective date)
MARKET_TRIGGERS = [
    ("reflected_in_trading", re.compile(r"reflected\s+in\s+the\s+trading", re.IGNORECASE)),
    ("split_adjusted", re.compile(r"split-?adjusted", re.IGNORECASE)),
    ("begin_trading", re.compile(r"(will\s+)?(begin|commence)\s+trading", re.IGNORECASE)),
    ("will_become_effective", re.compile(r"will\s+become\s+effective|become\s+effective", re.IGNORECASE)),
    ("effective_time", re.compile(r"effective\s+time", re.IGNORECASE)),
]

# LOW-signal / misleading triggers (approval / charter filing)
APPROVAL_TRIGGERS = [
    ("delaware", re.compile(r"state\s+of\s+delaware", re.IGNORECASE)),
    ("stockholders_approved", re.compile(r"stockholders\s+approved|shareholders\s+approved", re.IGNORECASE)),
    ("certificate_amendment", re.compile(r"certificate\s+of\s+amendment", re.IGNORECASE)),
]


# Month-name dates (extend if you also want numeric and ISO)
_DATE_ANY = re.compile(r"(?P<date>[A-Za-z]{3,9}\s+\d{1,2},?\s+\d{4})")

# High-signal "market/trading" triggers (priority order below)
_MARKET_TRIGGERS: List[Tuple[str, re.Pattern]] = [
    ("reflected_in_trading", re.compile(r"\breflected\s+in\s+the\s+trading\b", re.IGNORECASE)),
    ("begin_trading",        re.compile(r"\b(will\s+)?(begin|commence)\s+trading\b", re.IGNORECASE)),
    ("split_adjusted",       re.compile(r"\bsplit-?adjusted\b", re.IGNORECASE)),
    ("will_become_effective",re.compile(r"\bwill\s+become\s+effective\b|\bbecome\s+effective\b", re.IGNORECASE)),
    ("effective_time",       re.compile(r"\beffective\s+time\b", re.IGNORECASE)),
]

# Lower is better
_MARKET_PRIORITY = {
    "reflected_in_trading": 0,
    "begin_trading": 1,
    "split_adjusted": 2,
    "will_become_effective": 3,
    "effective_time": 4,
}

_PLACEHOLDER_RE = re.compile(r"\[.*?trading date.*?\]|\[expected.*?\]|\[.*?\]", re.IGNORECASE)

def extract_effective_date_market_priority(text: str, filed_at: Optional[datetime] = None) -> Optional[datetime]:
    """
    Market-first effective date extractor.

    Updated behavior:
      - Finds candidate dates that appear after high-signal market/trading triggers.
      - ALSO captures OTC/FINRA phrasing like "implemented effective December 18, 2025"
      - If filed_at is provided: DISALLOW any effective date that precedes the filing date.
        If the top candidate is before filed_at, it is dropped and we choose the next best.
        If no valid candidates remain, returns None.
      - Otherwise: choose best by trigger priority, then latest date within that priority.
    """
    if not text:
        return None

    t = _norm_text_html(text)
    tl = t.lower()

    candidates: List[Tuple[int, datetime, str]] = []  # (priority, dt, trigger_name)

    # 1) Market trigger scanning (your existing logic)
    for name, trig in _MARKET_TRIGGERS:
        pr = _MARKET_PRIORITY.get(name, 99)
        for m in trig.finditer(tl):
            start = m.start()
            window = t[start: min(len(t), start + 2500)]

            dm = _DATE_ANY.search(window)
            if not dm:
                continue

            raw = dm.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            candidates.append((pr, dt, name))

    # 2) NEW: “implemented effective …” style (ABQQ / FINRA language)
    # We treat this as MARKET strength (high priority).
    IMPLEMENTED_EFFECTIVE_PATTERNS = [
        re.compile(r"\bimplemented\s+effective\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE),
        re.compile(r"\bimplemented\s+on\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE),
        re.compile(r"\b(became|becomes)\s+effective\s+on\s+(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE),
        re.compile(r"\bmarket\s+effective\s+date\b.*?(?P<date>[A-Za-z]+\s+\d{1,2},\s+\d{4})\b", re.IGNORECASE | re.DOTALL),
    ]

    for pat in IMPLEMENTED_EFFECTIVE_PATTERNS:
        for m in pat.finditer(t):
            raw = m.group("date")
            try:
                dt = dtparser.parse(raw, fuzzy=True)
            except Exception:
                continue

            # priority 0 = "as good as it gets"
            candidates.append((0, dt, "implemented_effective"))

    if not candidates:
        return None

    # Hard rule: do not allow an effective date earlier than the filing date
    if filed_at is not None:
        filing_date = filed_at.date()
        candidates = [c for c in candidates if c[1].date() >= filing_date]
        if not candidates:
            return None

    # Rank: priority asc, then latest date desc
    def _rank(item: Tuple[int, datetime, str]) -> Tuple[int, float]:
        pr, dt, _name = item
        return (pr, -dt.timestamp())

    candidates.sort(key=_rank)
    return candidates[0][1]


# --- update extract_details to pass filed_at into extract_effective_date ---
def extract_details(text: str, filed_at: datetime) -> Extraction:
    ctx = extract_reverse_split_context(text)
    if not ctx or len(ctx) < 500:
        ctx = text

    ctx_l = ctx.lower()

    # Run rounding classification for BOTH reverse splits and share consolidations
    is_splitish = (
        "reverse stock split" in ctx_l
        or "reverse split" in ctx_l
        or "share consolidation" in ctx_l
        or "stock consolidation" in ctx_l
        or "post-consolidation" in ctx_l
        or "pre-consolidation" in ctx_l
    )

    if not is_splitish:
        rounding = UNKNOWN
    else:
        rounding = classify_rounding_policy(ctx)
        if rounding == UNKNOWN:
            rounding = classify_rounding_policy(text)  # fallback to full doc

    ratio_new, ratio_old = extract_ratio(ctx)
    # Context slicing can miss the definitive ratio in very large filings.
    # If we failed to parse a ratio from ctx, retry on the full document.
    if ratio_new is None or ratio_old is None:
        r2_new, r2_old = extract_ratio(text)
        if r2_new is not None and r2_old is not None:
            ratio_new, ratio_old = r2_new, r2_old

    # PRPH-first: try market-signal extraction on full text
    effective = extract_effective_date_market_priority(text, filed_at=filed_at)
    if effective is None:
        effective = extract_effective_date(text, filed_at=filed_at)

    return Extraction(
        ratio_new=ratio_new,
        ratio_old=ratio_old,
        effective_date=effective,
        rounding_policy=rounding,
        matches_rounding=rounding != UNKNOWN,
    )