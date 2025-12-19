import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional
import os
import re

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
        if self._mapping and self.path.exists() and (time.time() - self.path.stat().st_mtime) < 7*24*3600:
            return

        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        resp = _get_with_retries(
            session,
            url,
            headers=_sec_headers(user_agent),
            timeout=int(os.environ.get("SEC_TIMEOUT", "90")),
            retries=int(os.environ.get("SEC_RETRIES", "5")),
            backoff=float(os.environ.get("SEC_BACKOFF", "2.0")),
        )
        resp.raise_for_status()
        payload = resp.json()

        if isinstance(payload, dict) and "fields" in payload and "data" in payload:
            fields = payload["fields"]
            rows = payload["data"]
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
                    continue

            self.save()
            return

        it = payload.values() if isinstance(payload, dict) else payload if isinstance(payload, list) else []
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
        return self._mapping.get((cik or "").zfill(10), {})


FORMS_OF_INTEREST = [
    "8-K",
    "8-K/A"
]

_ACCESSION_RE = re.compile(r"(\d{10}-\d{2}-\d{6})")
_CIK_RE = re.compile(r"\((\d{10})\)")
_CIK_IN_LINK_RE = re.compile(r"/data/(\d{1,10})/")
_FORM_RE = re.compile(r"^([A-Z0-9\-\/ ]+)\s+-\s+")

def _first_link_href(entry) -> str:
    links = getattr(entry, "links", None)
    if links and isinstance(links, list):
        for l in links:
            href = l.get("href") if isinstance(l, dict) else None
            if href:
                return href
    return entry.get("link") or entry.get("id", "")

def _parse_entry(entry) -> Optional[Filing]:
    title = (entry.get("title") or "").strip()
    if not title:
        return None

    link = _first_link_href(entry)
    if not link:
        return None

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

    cik = ""
    m = _CIK_RE.search(title)
    if m:
        cik = m.group(1)
    else:
        m2 = _CIK_IN_LINK_RE.search(link)
        if m2:
            cik = str(int(m2.group(1))).zfill(10)
        else:
            cik = str(entry.get("cik", "")).zfill(10) if entry.get("cik") else ""

    form = ""
    m = _FORM_RE.match(title)
    if m:
        form = m.group(1).strip()
    else:
        form = entry.get("filing-type", "") or ""

    company = title
    if " - " in company:
        company = company.split(" - ", 1)[1]
    company = re.sub(r"\(\d{10}\)\s*\(Filer\)\s*$", "", company).strip()

    filed_str = entry.get("updated") or entry.get("published") or ""
    if filed_str:
        filed_at = datetime.strptime(filed_str.split("T")[0], "%Y-%m-%d")
    else:
        filed_at = datetime.utcnow()

    text_url = link.replace("-index.htm", ".txt")

    return Filing(
        accession=accession,
        cik=(cik or "").zfill(10),
        company=company,
        form=form,
        filed_at=filed_at,
        link=link,
        text_url=text_url,
    )

def _get_with_retries(session, url, headers, timeout=90, retries=5, backoff=2.0):
    last_exc = None
    for i in range(retries):
        try:
            resp = session.get(url, headers=headers, timeout=timeout)
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** i)
                continue
            return resp
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            last_exc = e
            time.sleep(backoff ** i)
            continue
    if last_exc:
        raise last_exc
    raise requests.exceptions.ReadTimeout(f"Failed to GET {url}")

def _sec_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept": "application/atom+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }

# ----------------------------
# Feed cache fallback
# ----------------------------

def _feed_cache_path(data_dir: Path) -> Path:
    return data_dir / "cache_feeds.json"

def _load_feed_cache(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            return {}
    return {}

def _save_feed_cache(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2))

def fetch_recent_filings(
    forms: Iterable[str],
    window_hours: int,
    session: requests.Session,
    user_agent: str,
    *,
    data_dir: Path = Path("data"),
    use_feed_cache_on_failure: bool = True,
) -> List[Filing]:
    cutoff = datetime.utcnow() - timedelta(hours=window_hours)
    filings: List[Filing] = []

    cache_path = _feed_cache_path(data_dir)
    feed_cache = _load_feed_cache(cache_path)

    sec_timeout = int(os.environ.get("SEC_TIMEOUT", "90"))
    sec_retries = int(os.environ.get("SEC_RETRIES", "5"))
    sec_backoff = float(os.environ.get("SEC_BACKOFF", "2.0"))
    sec_delay_s = float(os.environ.get("SEC_DELAY_S", "0.2"))

    for form in forms:
        url = (
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&owner=include"
            f"&type={form}&count=200&output=atom"
        )

        feed_text: Optional[str] = None
        try:
            resp = _get_with_retries(
                session,
                url,
                headers=_sec_headers(user_agent),
                timeout=sec_timeout,
                retries=sec_retries,
                backoff=sec_backoff,
            )
            resp.raise_for_status()
            feed_text = resp.text

            feed_cache[form] = {"fetched_at": time.time(), "text": feed_text}
            _save_feed_cache(cache_path, feed_cache)

            if sec_delay_s > 0:
                time.sleep(sec_delay_s)

        except Exception:
            if not use_feed_cache_on_failure:
                raise
            cached = feed_cache.get(form, {})
            feed_text = cached.get("text")
            if not feed_text:
                continue

        parsed = feedparser.parse(feed_text)
        for entry in parsed.entries:
            filing = _parse_entry(entry)
            if not filing:
                continue
            if filing.filed_at < cutoff:
                continue
            filings.append(filing)

    unique: Dict[str, Filing] = {f.accession: f for f in filings}
    return list(unique.values())


import re
import time
import requests
from typing import Optional

_SEC_DOC_RE = re.compile(r"<SEC-DOCUMENT>\s*([0-9\-]+)\.txt", re.IGNORECASE)

def _sec_doc_accession(text: str) -> Optional[str]:
    if not text:
        return None
    m = _SEC_DOC_RE.search(text[:2000])  # only need header
    return m.group(1) if m else None

def fetch_filing_text(filing: Filing, cache: FilingCache, session: requests.Session, user_agent: str) -> Optional[str]:
    """
    Fetch filing submission text. Cache-safe:
    - If cached blob does NOT match the requested accession in <SEC-DOCUMENT>, ignore it and refetch.
    """
    # 1) Check cache
    cached = cache.get(filing.accession)
    if cached:
        got = _sec_doc_accession(cached)
        if got == filing.accession:
            return cached
        # cache is poisoned/mismatched
        print(f"WARNING: cache mismatch for {filing.accession}: cached SEC-DOC={got}. Refetching...")

    # 2) Fetch with retries
    url = filing.text_url
    for i in range(4):
        try:
            resp = session.get(url, headers=_sec_headers(user_agent), timeout=90)
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** i)
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            time.sleep(2 ** i)
            continue

        if resp.status_code != 200:
            return None

        text = resp.text or ""
        got = _sec_doc_accession(text)
        if got and got != filing.accession:
            # Don’t cache wrong content
            print(f"WARNING: fetched SEC-DOC mismatch for {filing.accession}: got {got}. Not caching.")
            return text

        cache.set(filing.accession, text)
        return text

    return None


# ---------------------------------------------------------------------------
# CIK-direct fetch via SEC submissions JSON (reliable for a specific company)
# ---------------------------------------------------------------------------

def _accession_nodash(accession: str) -> str:
    return (accession or "").replace("-", "").strip()

def fetch_company_tickers_from_submissions(cik: str, session: requests.Session, user_agent: str) -> List[str]:
    cik10 = str(int(cik)).zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    headers = {"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"}
    r = session.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    j = r.json()
    return [t.upper() for t in (j.get("tickers") or []) if isinstance(t, str)]


def fetch_company_filings_from_submissions(
    cik: str,
    session: requests.Session,
    user_agent: str,
    *,
    forms: Optional[Iterable[str]] = None,
    window_days: int = 7,
    limit: int = 50,
) -> List[Filing]:
    """
    Pull a company's recent filings directly from SEC submissions JSON.

    This is much more reliable than the 'getcurrent' atom feed when you need to
    guarantee coverage for a specific CIK (e.g., BENF).

    Returns Filing objects with:
      - link: the filing index html
      - text_url: the full submission text file (accession_nodash.txt)
    """
    cik10 = (cik or "").zfill(10)
    cik_int = str(int(cik10))  # strip leading zeros for Archives path
    cutoff = datetime.utcnow() - timedelta(days=window_days)

    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    resp = _get_with_retries(
        session,
        url,
        headers=_sec_headers(user_agent),
        timeout=int(os.environ.get("SEC_TIMEOUT", "90")),
        retries=int(os.environ.get("SEC_RETRIES", "5")),
        backoff=float(os.environ.get("SEC_BACKOFF", "2.0")),
    )
    resp.raise_for_status()
    payload = resp.json()

    company_name = (payload.get("name") or "").strip() or cik10

    recent = (payload.get("filings") or {}).get("recent") or {}
    forms_arr = recent.get("form") or []
    acc_arr = recent.get("accessionNumber") or []
    date_arr = recent.get("filingDate") or []
    prim_arr = recent.get("primaryDocument") or []

    want_forms = set(f.strip().upper() for f in forms) if forms else None

    out: List[Filing] = []
    n = min(len(forms_arr), len(acc_arr), len(date_arr), len(prim_arr), limit)

    for i in range(n):
        form = str(forms_arr[i] or "").strip().upper()
        accession = str(acc_arr[i] or "").strip()
        filed_str = str(date_arr[i] or "").strip()
        _primary_doc = str(prim_arr[i] or "").strip()

        if want_forms and form not in want_forms:
            continue

        try:
            filed_at = datetime.strptime(filed_str, "%Y-%m-%d")
        except Exception:
            filed_at = datetime.utcnow()

        if filed_at < cutoff:
            continue

        acc_no = _accession_nodash(accession)
        if not acc_no:
            continue

        link = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no}/{accession}-index.html"
        # SEC "full submission text" is usually the dashed accession filename
        text_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int}/{acc_no}/{accession}.txt"


        out.append(
            Filing(
                accession=accession,
                cik=cik10,
                company=company_name,
                form=form,
                filed_at=filed_at,
                link=link,
                text_url=text_url,
            )
        )

    uniq = {f.accession: f for f in out}
    return list(uniq.values())


from math import ceil

def _company_tickers_cache_path(data_dir: Path) -> Path:
    return data_dir / "company_tickers_exchange_cache.json"

def _universe_cursor_path(data_dir: Path) -> Path:
    return data_dir / "universe_cursor.json"

def _load_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return default

def _save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8")

def get_cik_universe(
    session: requests.Session,
    user_agent: str,
    *,
    data_dir: Path = Path("data"),
    refresh_hours: int = 24,
) -> List[str]:
    """
    Returns a sorted list of CIKs (10-digit strings) from SEC's
    company_tickers_exchange.json, cached on disk.
    """
    cache_path = _company_tickers_cache_path(data_dir)
    cache = _load_json(cache_path, {"fetched_at": 0, "payload": None})

    now = time.time()
    payload = cache.get("payload")
    fetched_at = cache.get("fetched_at", 0)

    if (not payload) or (now - fetched_at > refresh_hours * 3600):
        url = "https://www.sec.gov/files/company_tickers_exchange.json"
        resp = _get_with_retries(
            session,
            url,
            headers=_sec_headers(user_agent),
            timeout=int(os.environ.get("SEC_TIMEOUT", "90")),
            retries=int(os.environ.get("SEC_RETRIES", "5")),
            backoff=float(os.environ.get("SEC_BACKOFF", "2.0")),
        )
        resp.raise_for_status()
        payload = resp.json()
        cache = {"fetched_at": now, "payload": payload}
        _save_json(cache_path, cache)

    ciks: List[str] = []

    # Newer SEC shape: {"fields":[...], "data":[...]}
    if isinstance(payload, dict) and "fields" in payload and "data" in payload:
        fields = payload["fields"]
        rows = payload["data"]
        idx = {name: i for i, name in enumerate(fields)}
        cik_i = idx.get("cik")
        if cik_i is None:
            return []
        for row in rows:
            try:
                cik_val = row[cik_i]
                if cik_val is None:
                    continue
                ciks.append(str(int(cik_val)).zfill(10))
            except Exception:
                continue
    else:
        # older shape: list/dict of objects with cik_str
        iterable = payload.values() if isinstance(payload, dict) else payload
        for entry in iterable:
            if not isinstance(entry, dict):
                continue
            cik_val = entry.get("cik") or entry.get("cik_str")
            if cik_val is None:
                continue
            try:
                ciks.append(str(int(cik_val)).zfill(10))
            except Exception:
                continue

    # de-dupe + stable order
    ciks = sorted(set(ciks))
    return ciks

def fetch_recent_filings_via_submissions_universe(
    forms: Iterable[str],
    window_hours: int,
    session: requests.Session,
    user_agent: str,
    *,
    data_dir: Path = Path("data"),
    batch_size: int = 999999,
    limit_per_cik: int = 25,
    window_days_floor: int = 2,
    debug: bool = True,
) -> List[Filing]:
    """
    Option B: Universe scan using submissions JSON (no /Archives daily index).
    We scan a rolling batch of CIKs per run, storing a cursor in data/universe_cursor.json.
    """
    # Convert hours to days for submissions window
    window_days = max(window_days_floor, ceil(window_hours / 24))

    # Load universe
    universe = get_cik_universe(session, user_agent, data_dir=data_dir)
    print("Universe size ", len(universe))
    batch_size = min(batch_size, len(universe))
    if not universe:
        if debug:
            print("[universe] empty universe; cannot scan")
        return []

    # Cursor state
    cursor_path = _universe_cursor_path(data_dir)
    state = _load_json(cursor_path, {"cursor": 0})
    cursor = int(state.get("cursor", 0)) % max(1, len(universe))

    # Slice batch and wrap
    batch = universe[cursor:cursor + batch_size]
    if len(batch) < batch_size:
        batch += universe[0:max(0, batch_size - len(batch))]

    # Advance cursor
    new_cursor = (cursor + batch_size) % len(universe)
    _save_json(cursor_path, {"cursor": new_cursor, "universe_size": len(universe), "updated_at": time.time()})

    if debug:
        print(f"[universe] scanning batch_size={len(batch)} cursor={cursor}->{new_cursor} universe_size={len(universe)} window_days={window_days}")

    out: List[Filing] = []
    sec_delay_s = float(os.environ.get("SEC_DELAY_S", "0.4"))

    for i, cik in enumerate(batch, 1):
        try:
            more = fetch_company_filings_from_submissions(
                cik,
                session,
                user_agent,
                forms=forms,
                window_days=window_days,
                limit=limit_per_cik,
            )
            out.extend(more)
        except Exception:
            # keep going; one issuer failing shouldn't kill the run
            pass

        if sec_delay_s > 0:
            time.sleep(sec_delay_s)

        if debug and i % 200 == 0:
            print(f"[universe] scanned {i}/{len(batch)} CIKs; filings={len(out)}")

    # De-dupe by accession
    uniq = {f.accession: f for f in out if f and f.accession}
    return list(uniq.values())
