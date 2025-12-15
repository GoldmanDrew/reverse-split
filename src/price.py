import csv
import json
from datetime import date
from pathlib import Path
from typing import Dict, Optional

import requests

# yfinance remains available for callers that still rely on it, but new
# functionality below uses the lighter-weight Stooq endpoint.
import yfinance as yf


class PriceCache:
    def __init__(self, path: Path):
        self.path = path
        self._data: Dict[str, Dict[str, float]] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._data = json.loads(self.path.read_text())
            except json.JSONDecodeError:
                self._data = {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._data, indent=2))

    def get(self, ticker: str, as_of: date) -> Optional[float]:
        record = self._data.get(ticker.upper(), {})
        return record.get(as_of.isoformat())

    def set(self, ticker: str, as_of: date, price: float) -> None:
        ticker_key = ticker.upper()
        self._data.setdefault(ticker_key, {})[as_of.isoformat()] = price


def fetch_stooq_close(ticker: str, cache: PriceCache, session: Optional[requests.Session] = None) -> Optional[float]:
    """
    Fetch the latest close price from Stooq.

    Stooq's CSV endpoint is lightweight and doesn't require auth. We cache prices
    by ticker and date to avoid repeated network calls. This should be invoked
    *after* all policy filters have already been applied.
    """

    today = date.today()
    cached = cache.get(ticker, today)
    if cached is not None:
        return cached

    ticker_key = (ticker or "").strip().lower()
    if not ticker_key:
        return None

    symbol = f"{ticker_key}.us"
    url = f"https://stooq.pl/q/l/?s={symbol}&f=sd2t2ohlcv&h&e=csv"
    sess = session or requests.Session()

    try:
        resp = sess.get(url, timeout=10)
    except requests.RequestException:
        return None

    if resp.status_code != 200 or not resp.text:
        return None

    reader = csv.DictReader(resp.text.splitlines())
    row = next(reader, None)
    if not row:
        return None

    close_raw = row.get("Close") or row.get("close")
    try:
        close = float(close_raw)
    except (TypeError, ValueError):
        return None

    cache.set(ticker, today, close)
    return close


def fetch_close_price(ticker: str, cache: PriceCache) -> Optional[float]:
    today = date.today()
    cached = cache.get(ticker, today)
    if cached is not None:
        return cached
    try:
        data = yf.Ticker(ticker).history(period="5d")
    except Exception:
        return None
    if data.empty:
        return None
    last_close = float(data["Close"].iloc[-1])
    cache.set(ticker, today, last_close)
    return last_close


def fetch_price_with_fallback(
    ticker: str, cache: PriceCache, session: Optional[requests.Session] = None
) -> Optional[float]:
    """Try Stooq first, then fall back to Yahoo Finance if missing."""

    stooq_px = fetch_stooq_close(ticker, cache, session=session)
    if stooq_px is not None:
        return stooq_px

    return fetch_close_price(ticker, cache)
