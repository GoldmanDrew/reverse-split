import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import feedparser
import requests

USER_AGENT = "reverse-split-monitor/0.1 (contact@example.com)"


@dataclass
class Filing:
    accession: str
    cik: str
    company: str
    form: str
    filed_at: datetime
    link: str
    text_url: str


class FilingCache:
    def __init__(self, path: Path):
        self.path = path
        self._data: Dict[str, str] = {}
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

    def get(self, accession: str) -> Optional[str]:
        return self._data.get(accession)

    def set(self, accession: str, text: str) -> None:
        self._data[accession] = text


class SeenAccessions:
    def __init__(self, path: Path):
        self.path = path
        self._seen: Dict[str, float] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._seen = json.loads(self.path.read_text())
            except json.JSONDecodeError:
                self._seen = {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._seen, indent=2))

    def add(self, accession: str) -> None:
        self._seen[accession] = time.time()

    def __contains__(self, accession: str) -> bool:
        return accession in self._seen


class TickerMap:
    def __init__(self, path: Path):
        self.path = path
        self._mapping: Dict[str, Dict[str, str]] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                self._mapping = json.loads(self.path.read_text())
            except json.JSONDecodeError:
                self._mapping = {}

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._mapping, indent=2))

    def refresh(self, session: requests.Session, user_agent: str) -> None:
        if self._mapping:
            return
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        resp = session.get(url, headers=_sec_headers(user_agent), timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for entry in data.values():
            cik_str = str(entry["cik_str"]).zfill(10)
            self._mapping[cik_str] = {
                "ticker": entry.get("ticker", "").upper(),
                "exchange": entry.get("exchange", "").upper(),
                "title": entry.get("title", ""),
            }
        self.save()

    def lookup(self, cik: str) -> Dict[str, str]:
        return self._mapping.get(cik, {})


FORMS_OF_INTEREST = [
    "8-K",
    "8-K/A",
    "DEF 14A",
    "PRE 14A",
    "S-1",
    "S-1/A",
    "F-1",
    "F-1/A",
]


def _parse_entry(entry) -> Optional[Filing]:
    accession = entry.get("accession-number") or entry.get("accessionnumber")
    if not accession:
        # fallback: try from id
        entry_id = entry.get("id", "")
        parts = entry_id.split("/")
        if parts:
            accession = parts[-1].replace("-index.htm", "")
    link = entry.get("link") or entry.get("id", "")
    if not accession or not link:
        return None
    text_url = link.replace("-index.htm", ".txt")
    cik = entry.get("cik", "").zfill(10)
    company = entry.get("company-name", entry.get("title", "")).strip()
    form = entry.get("category", {}).get("term") or entry.get("filing-type", "")
    filed_str = entry.get("filing-date") or entry.get("updated")
    filed_at = datetime.strptime(filed_str.split("T")[0], "%Y-%m-%d") if filed_str else datetime.utcnow()
    return Filing(
        accession=accession,
        cik=cik,
        company=company,
        form=form,
        filed_at=filed_at,
        link=link,
        text_url=text_url,
    )


def fetch_recent_filings(forms: Iterable[str], window_hours: int, session: requests.Session, user_agent: str) -> List[Filing]:
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)
    filings: List[Filing] = []
    for form in forms:
        url = (
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&owner=include"
            f"&type={form}&count=200&output=atom"
        )
        feed = session.get(url, headers=_sec_headers(user_agent), timeout=30)
        feed.raise_for_status()
        parsed = feedparser.parse(feed.text)
        for entry in parsed.entries:
            filing = _parse_entry(entry)
            if not filing:
                continue
            if filing.filed_at < cutoff:
                continue
            filings.append(filing)
    # Remove duplicates by accession
    unique: Dict[str, Filing] = {f.accession: f for f in filings}
    return list(unique.values())


def fetch_filing_text(filing: Filing, cache: FilingCache, session: requests.Session, user_agent: str) -> Optional[str]:
    cached = cache.get(filing.accession)
    if cached:
        return cached
    resp = session.get(filing.text_url, headers=_sec_headers(user_agent), timeout=30)
    if resp.status_code != 200:
        return None
    text = resp.text
    cache.set(filing.accession, text)
    return text


def _sec_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

