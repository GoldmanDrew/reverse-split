#!/usr/bin/env python3

import os
import re
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json

import feedparser
import yfinance as yf

# ==========================
# CONFIG
# ==========================

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

# Sender (Gmail with App Password)
SENDER_EMAIL = os.environ.get("ALERT_SENDER_EMAIL")
SENDER_APP_PASSWORD = os.environ.get("ALERT_SENDER_APP_PWD")

# Recipients
RECIPIENTS = [
    "dag5wd@virginia.edu",
    "lcordover14@gmail.com",
]

# How far back to look each run
LOOKBACK_HOURS = 4

# Track already-alerted links
STATE_FILE = Path("reverse_split_seen.json")

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

# ==========================
# TIME FILTER
# ==========================

def entry_time_within_lookback(entry, lookback_hours):
    if not hasattr(entry, "published_parsed") or entry.published_parsed is None:
        # If no timestamp, keep (we prefer false positives)
        return True
    pub_dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    return pub_dt >= cutoff

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
        return True  # cannot safely filter, keep it

    try:
        tk = yf.Ticker(symbol)
        hist = tk.history(period="1d")
        if hist.empty:
            return True
        price = float(hist["Close"].iloc[-1])
    except Exception:
        return True  # network/API/other issues → do not exclude

    theoretical_post = price * ratio

    if theoretical_post < min_post_price:
        return False

    return True

# ==========================
# ROUND-UP DETECTION
# ==========================

def looks_like_roundup_case(entry):
    text_raw = f"{entry.title} {getattr(entry, 'summary', '')}"
    text = text_raw.lower()

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
        return False
    if any(k in text for k in canadian_keywords):
        return False
    if any(k in text for k in etf_keywords):
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
    if not SENDER_EMAIL or not SENDER_APP_PASSWORD:
        raise RuntimeError("Set ALERT_SENDER_EMAIL and ALERT_SENDER_APP_PWD environment variables first.")

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
            continue

        entry_id = link

        if entry_id in seen_ids:
            continue
        if not entry_time_within_lookback(e, LOOKBACK_HOURS):
            continue
        if not looks_like_roundup_case(e):
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

    save_seen_ids(seen_ids)

if __name__ == "__main__":
    main()
