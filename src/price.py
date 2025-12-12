import json
from datetime import date
from pathlib import Path
from typing import Dict, Optional

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
