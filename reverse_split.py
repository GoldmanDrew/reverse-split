#!/usr/bin/env python3

import json
import logging
import os
import re
import smtplib
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from pathlib import Path

import feedparser
import yfinance as yf

# ==========================
# CONFIG / LOGGING
# ==========================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
)

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Sender (Gmail with App Password)
SENDER_EMAIL = os.environ.get("ALERT_SENDER_EMAIL", "werdnamdlog01@gmail.com")
SENDER_APP_PASSWORD = os.environ.get("ALERT_SENDER_APP_PWD")

# Recipients
RECIPIENTS = [
    "dag5wd@virginia.edu",
    "lcordover14@gmail.com",
]

# Track already-alerted links
STATE_FILE = Path("reverse_split_seen.json")

# Human-readable record of alerts that passed all filters in the latest run
RESULTS_FILE = Path("reverse_split_results.json")

# Wide, false-positive-biased Google News queries
SEARCH_QUERIES = [
    '"reverse stock split" "rounded up to the nearest whole share"',
    '"reverse stock split" "fractional shares will be rounded up"',
    '"reverse split" "rounded up to the nearest whole share"',
    '"no fractional shares will be issued" "rounded up to the nearest whole share"',
    '"share consolidation" "no fractional shares will be issued" "one share in lieu of the fractional share"',
    '"share consolidation" "each shareholder will be entitled to receive one share" "in lieu of the fractional share"',
    '"share consolidation" "no fractional shares will be issued to any shareholders" "one share in lieu"',
    '"reverse stock split" "in lieu of the fractional share"',
]

# ==========================
# STATE HELPERS
# ==========================

def load_seen_ids():
    if STATE_FILE.exists():
        try:
            return set(json.loads(STATE_FILE.read_text()))
        except Exception:
            return set()
    return set()

def save_seen_ids(seen_ids):
    STATE_FILE.write_text(json.dumps(sorted(list(seen_ids))))


def save_results(items):
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "count": len(items),
        "items": items,
    }
    RESULTS_FILE.write_text(json.dumps(payload, indent=2))
    logging.info("Wrote %s items to %s", len(items), RESULTS_FILE)

# ==========================
# FETCH FEED ENTRIES
# ==========================

def fetch_entries():
    entries = []
    for q in SEARCH_QUERIES:
        url_q = q.replace(" ", "+")
        rss_url = f"https://news.google.com/rss/search?q={url_q}&hl=en-US&gl=US&ceid=US:en"
        feed = feedparser.parse(rss_url)
        entries.extend(feed.entries)
    return entries


def entry_label(entry) -> str:
    title = getattr(entry, "title", "(no title)")
    link = getattr(entry, "link", "")
    if link:
        return f"{title} [{link}]"
    return title

# ==========================
# DATE PARSING (EFFECTIVE WITHIN 5 DAYS)
# ==========================

MONTH_MAP = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

MONTH_DAY_PATTERN = re.compile(
    r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t)?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\.?\s+(\d{1,2})(?:st|nd|rd|th)?(?:,?\s+(\d{2,4}))?",
    re.IGNORECASE,
)

NUMERIC_DATE_PATTERN = re.compile(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b")


def _normalize_year(year_text: str | None, today_year: int) -> int:
    if not year_text:
        return today_year
    try:
        year = int(year_text)
        if year < 100:
            return 2000 + year
        return year
    except ValueError:
        return today_year


def _iter_candidate_dates(text: str):
    """Yield date objects parsed from common month/day/year patterns."""

    today_year = datetime.now(timezone.utc).year

    for m in MONTH_DAY_PATTERN.finditer(text):
        month_key = m.group(1).lower()[:3]
        month = MONTH_MAP.get(month_key)
        day = int(m.group(2))
        year = _normalize_year(m.group(3), today_year)
        if month:
            yield datetime(year, month, day, tzinfo=timezone.utc).date()

    for m in NUMERIC_DATE_PATTERN.finditer(text):
        month = int(m.group(1))
        day = int(m.group(2))
        year = _normalize_year(m.group(3), today_year)
        try:
            yield datetime(year, month, day, tzinfo=timezone.utc).date()
        except ValueError:
            continue

    lowered = text.lower()
    today = datetime.now(timezone.utc).date()
    if "tomorrow" in lowered:
        yield today + timedelta(days=1)
    if "today" in lowered:
        yield today


def event_within_next_five_days(entry):
    """Return True if we can infer the split happens within the next 5 days."""

    window_start = datetime.now(timezone.utc).date()
    window_end = window_start + timedelta(days=5)
    text = f"{entry.title} {getattr(entry, 'summary', '')}"

    for candidate in _iter_candidate_dates(text):
        if window_start <= candidate <= window_end:
            logging.info("Split date %s within window for entry: %s", candidate, entry_label(entry))
            return True
        if candidate < window_start:
            logging.info("Filtered past-dated split (%s) for entry: %s", candidate, entry_label(entry))
        else:
            logging.info("Filtered split outside 5-day window (%s) for entry: %s", candidate, entry_label(entry))

    logging.info("No effective date within 5 days found for entry: %s", entry_label(entry))
    return False

# ==========================
# RATIO / TICKER / PRICE HELPERS
# ==========================

def extract_split_ratio(text: str):
    """
    Extract reverse split ratio denominator N from patterns like:
    '1-for-40', '1 for 50', 'one-for-20'.
    Returns int or None.
    """
    text = text.lower()
    text = text.replace("one-for", "1-for").replace("one for", "1 for")

    m = re.search(r"1\s*[- ]\s*for\s*[- ]\s*(\d+)", text)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def extract_ticker(text: str):
    """
    Very rough ticker extraction from common patterns like:
    '(Nasdaq: NVVE)', '(NYSE: ABC)', '(NYSE American: XYZ)'.
    Returns ticker string or None.
    """
    text = text.replace("\u2013", "-")  # normalize en dash
    m = re.search(r"\((nasdaq|nyse american|nyse|amex):\s*([A-Z\.]+)\)", text, re.IGNORECASE)
    if m:
        return m.group(2).upper()
    return None


def passes_price_threshold(text: str, min_post_price: float = 1.0) -> bool:
    """
    Exclude clear junk: if we can determine that
        last_price * split_ratio < min_post_price
    then return False.

    If we can't confidently parse ratio/ticker/price, return True
    so we do NOT accidentally drop real opportunities.
    """
    ratio = extract_split_ratio(text)
    symbol = extract_ticker(text)

    if not ratio or not symbol:
        if not ratio:
            logging.info("Skipping price filter (no ratio found) for text starting: %s", text[:120])
        if not symbol:
            logging.info("Skipping price filter (no ticker found) for text starting: %s", text[:120])
        return True  # cannot safely filter, keep it

    try:
        tk = yf.Ticker(symbol)
        hist = tk.history(period="1d")
        if hist.empty:
            logging.info("Price history empty for %s; keeping entry", symbol)
            return True
        price = float(hist["Close"].iloc[-1])
    except Exception as exc:
        logging.info("Could not fetch price for %s (%s); keeping entry", symbol, exc)
        return True  # network/API/other issues → do not exclude

    theoretical_post = price * ratio

    if theoretical_post < min_post_price:
        logging.info(
            "Filtered %s: ratio %s, last %.2f, theoretical post-split %.2f < threshold %.2f",
            symbol,
            ratio,
            price,
            theoretical_post,
            min_post_price,
        )
        return False

    return True

# ==========================
# ROUND-UP DETECTION
# ==========================

def looks_like_roundup_case(entry):
    text_raw = f"{entry.title} {getattr(entry, 'summary', '')}"
    text = text_raw.lower()
    label = entry_label(entry)

    # -----------------------------------------------------------
    # EXCLUSIONS: ADRs, Canadian companies, ETFs
    # -----------------------------------------------------------
    adr_keywords = [
        "american depositary", "american depository", "adr",
        "ads", "adrs", "sponsored adr", "unsponsored adr",
        "adr program", "depositary bank",
    ]

    canadian_keywords = [
        "tsx", "cse", "canadian securities exchange",
        "toronto stock exchange", "vancouver",
        ".to", ".v", "(tsx", "(cse",
    ]

    etf_keywords = [
        "etf", "exchange traded fund", "trust units",
        "unit holders", "fund units", "series units",
        "trust", "index fund",
    ]

    if any(k in text for k in adr_keywords):
        logging.info("Filtered ADR-related entry: %s", label)
        return False
    if any(k in text for k in canadian_keywords):
        logging.info("Filtered Canadian listing: %s", label)
        return False
    if any(k in text for k in etf_keywords):
        logging.info("Filtered ETF/trust language: %s", label)
        return False

    # -----------------------------------------------------------
    # Must refer to reverse split / consolidation
    # -----------------------------------------------------------
    if not any(kw in text for kw in [
        "reverse stock split",
        "reverse split",
        "share consolidation",
        "share combination",
    ]):
        logging.info("Filtered non-split entry: %s", label)
        return False

    # -----------------------------------------------------------
    # Direct round-up language
    # -----------------------------------------------------------
    roundup_keywords = [
        "rounded up to the nearest whole share",
        "rounded up to the nearest whole",
        "fractional shares will be rounded up",
        "fractional shares resulting from the reverse stock split will be rounded up",
        "fractional shares resulting from the reverse split will be rounded up",
        "fractional shares resulting from the share consolidation will be rounded up",
        "rounded up to the next whole share",
    ]
    if any(k in text for k in roundup_keywords):
        if not passes_price_threshold(text_raw):
            return False
        return True

    # -----------------------------------------------------------
    # NVVE / BamSEC style: “no fractional shares” + “one share in lieu”
    # -----------------------------------------------------------
    if "no fractional shares will be issued" in text and "in lieu of the fractional share" in text:
        if not passes_price_threshold(text_raw):
            return False
        return True

    if "no fractional shares will be issued" in text and "each shareholder will be entitled to receive one share" in text:
        if not passes_price_threshold(text_raw):
            return False
        return True

    if "no fractional shares will be issued to any shareholders" in text and "one share in lieu" in text:
        if not passes_price_threshold(text_raw):
            return False
        return True

    # Loose fallback: reverse stock split + "in lieu of the fractional"
    if "reverse stock split" in text and "in lieu of the fractional" in text:
        if not passes_price_threshold(text_raw):
            return False
        return True

    return False

# ==========================
# EMAIL
# ==========================

def format_email(new_items):
    lines = [
        "Reverse Split Round-Up Alert",
        "",
        "The script detected possible reverse stock split opportunities",
        "where fractional shares appear to be rounded up.",
        "False positives are expected by design; always verify filings.",
        "",
    ]
    for i, item in enumerate(new_items, 1):
        lines.append(f"{i}. {item['title']}")
        lines.append(f"   Published: {item['published']}")
        lines.append(f"   Link: {item['link']}")
        lines.append("")
    return "\n".join(lines)

def send_email(subject, body):
    if not SENDER_APP_PASSWORD:
        raise RuntimeError(
            "Set ALERT_SENDER_APP_PWD with the Gmail app password for the sender account."
        )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(RECIPIENTS)

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SENDER_EMAIL, SENDER_APP_PASSWORD)
        server.sendmail(SENDER_EMAIL, RECIPIENTS, msg.as_string())

# ==========================
# MAIN
# ==========================

def main():
    seen_ids = load_seen_ids()
    entries = fetch_entries()

    new_items = []

    for e in entries:
        link = getattr(e, "link", None)
        if not link:
            logging.info("Skipping entry without link: %s", entry_label(e))
            continue

        entry_id = link

        if entry_id in seen_ids:
            logging.info("Already processed entry: %s", entry_id)
            continue
        if not event_within_next_five_days(e):
            continue
        if not looks_like_roundup_case(e):
            logging.info("Entry did not match roundup filters: %s", entry_id)
            continue

        new_items.append({
            "id": entry_id,
            "title": e.title,
            "link": link,
            "published": getattr(e, "published", "unknown"),
        })
        seen_ids.add(entry_id)

    if new_items:
        subject = f"[Reverse Split Alert] {len(new_items)} potential rounding-up events"
        body = format_email(new_items)
        send_email(subject, body)

    save_results(new_items)

    save_seen_ids(seen_ids)

if __name__ == "__main__":
    main()
