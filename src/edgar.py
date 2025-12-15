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
        # refresh if empty OR older than 7 days
        if self._mapping and self.path.exists() and (time.time() - self.path.stat().st_mtime) < 7*24*3600:
            return

        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        resp = session.get(url, headers=_sec_headers(user_agent), timeout=30)
        resp.raise_for_status()
        payload = resp.json()

        # New SEC format:
        # {"fields":["cik","name","ticker","exchange"], "data":[[cik,name,ticker,exchange], ...]}
        if isinstance(payload, dict) and "fields" in payload and "data" in payload:
            fields = payload["fields"]
            rows = payload["data"]

            # Map field name -> column index
            idx = {name: i for i, name in enumerate(fields)}

            cik_i = idx.get("cik")
            name_i = idx.get("name")
            ticker_i = idx.get("ticker")
            exch_i = idx.get("exchange")

            for row in rows:
                try:
                    cik_val = row[cik_i] if cik_i is not None else None
                    if cik_val is None:
                        continue
                    cik_str = str(int(cik_val)).zfill(10)

                    self._mapping[cik_str] = {
                        "ticker": (row[ticker_i] or "").upper() if ticker_i is not None else "",
                        "exchange": (row[exch_i] or "").upper() if exch_i is not None else "",
                        "title": row[name_i] if name_i is not None else "",
                    }
                except Exception:
                    # skip malformed rows
                    continue

            self.save()
            return

        # Old SEC formats (fallbacks):
        # - dict keyed by row number {"0":{"cik_str":...}, ...}
        # - list of dicts [{"cik_str":...}, ...]
        if isinstance(payload, dict):
            it = payload.values()
        elif isinstance(payload, list):
            it = payload
        else:
            it = []

        for entry in it:
            if not isinstance(entry, dict):
                continue
            cik_str = str(entry.get("cik_str") or entry.get("cik") or "").zfill(10)
            if not cik_str.strip("0"):
                continue
            self._mapping[cik_str] = {
                "ticker": str(entry.get("ticker", "")).upper(),
                "exchange": str(entry.get("exchange", "")).upper(),
                "title": str(entry.get("title") or entry.get("name") or ""),
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

import re

_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")
_CIK_RE = re.compile(r"\((\d{10})\)")
_FORM_RE = re.compile(r"^([A-Z0-9\-\/ ]+)\s+-\s+")

def _first_link_href(entry) -> str:
    # feedparser entries often have .links list, with dicts containing 'href'
    links = getattr(entry, "links", None)
    if links and isinstance(links, list):
        for l in links:
            href = l.get("href") if isinstance(l, dict) else None
            if href:
                return href
    # fallback
    return entry.get("link") or entry.get("id", "")

def _parse_entry(entry) -> Optional[Filing]:
    title = (entry.get("title") or "").strip()
    if not title:
        return None

    link = _first_link_href(entry)
    if not link:
        return None

    # Accession: prefer extracting from link, else from entry.id urn
    accession = None
    m = _ACCESSION_RE.search(link)
    if m:
        accession = m.group(1)
    else:
        entry_id = entry.get("id", "")
        if entry_id and "accession-number=" in entry_id:
            accession = entry_id.split("accession-number=")[-1].strip()

    if not accession:
        return None

    # CIK: often present in title like "(0001083743)"
    cik = ""
    m = _CIK_RE.search(title)
    if m:
        cik = m.group(1)
    else:
        # sometimes feedparser provides cik in other fields; keep fallback
        cik = str(entry.get("cik", "")).zfill(10) if entry.get("cik") else ""

    # Form: usually prefix of title like "8-K - Company (CIK) (Filer)"
    form = ""
    m = _FORM_RE.match(title)
    if m:
        form = m.group(1).strip()
    else:
        # fallback: feedparser sometimes provides tags/categories in other places
        form = entry.get("filing-type", "") or ""

    # Company name: strip leading "FORM - " and trailing "(CIK) (Filer)"
    company = title
    if " - " in company:
        company = company.split(" - ", 1)[1]
    # remove CIK and trailing parentheses bits
    company = re.sub(r"\(\d{10}\)\s*\(Filer\)\s*$", "", company).strip()

    # Filed date: Atom 'updated' exists and is reliable enough for window filter
    filed_str = entry.get("updated") or entry.get("published") or ""
    if filed_str:
        filed_at = datetime.strptime(filed_str.split("T")[0], "%Y-%m-%d")
    else:
        filed_at = datetime.utcnow()

    text_url = link.replace("-index.htm", ".txt")

    return Filing(
        accession=accession,
        cik=cik.zfill(10) if cik else "",
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

