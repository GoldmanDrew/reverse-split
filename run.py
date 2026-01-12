#!/usr/bin/env python3
import os
from datetime import datetime
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

import requests
import re
from src import alert, edgar, filters, parse, price

DATA_DIR = Path("data")
RESULTS_JSON = DATA_DIR / "results.json"
RESULTS_CSV = DATA_DIR / "results.csv"
CACHE_FILINGS = DATA_DIR / "cache_filings.json"
SEEN_ACCESSIONS = DATA_DIR / "seen_accessions.json"
TICKER_MAP_PATH = DATA_DIR / "ticker_map.json"
PRICE_CACHE_PATH = Path("price_cache.json")
REJECTIONS_JSON = DATA_DIR / "rejections.json"
REJECTIONS_CSV = DATA_DIR / "rejections.csv"

WINDOW_HOURS = 84

USER_AGENT = edgar.USER_AGENT

FORCE_REPROCESS = 1

LIMIT_PER_CIK = 8

def today_et():
    return datetime.now(ZoneInfo("America/New_York")).date()


def derive_common_ticker_from_map(ticker_map: str) -> str | None:
    """
    If SEC's single-ticker mapping points to a non-common instrument (often warrants),
    try a simple transformation to the common ticker.

    Example: BENFW -> BENF
    """
    t = (ticker_map or "").upper().strip()
    if t.endswith("W") and len(t) >= 2:
        return t[:-1]
    return None


class Runner:
    def __init__(self):
        if not USER_AGENT or "@" not in USER_AGENT:
            raise ValueError(
                "SEC_USER_AGENT must include contact information (name and email) to avoid SEC 403 responses."
            )

        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

        self.filing_cache = edgar.FilingCache(CACHE_FILINGS)
        self.seen = edgar.SeenAccessions(SEEN_ACCESSIONS)
        self.tickers = edgar.TickerMap(TICKER_MAP_PATH)
        self.tickers.refresh(self.session, USER_AGENT)

        self.price_cache = price.PriceCache(PRICE_CACHE_PATH)

    def run(self) -> List[dict]:
        # Option B: universe scan via submissions JSON (no daily index)
        filings = edgar.fetch_recent_filings_via_submissions_universe(
            edgar.FORMS_OF_INTEREST,
            WINDOW_HOURS,
            self.session,
            USER_AGENT,
            data_dir=DATA_DIR,
            batch_size= 999999,
            limit_per_cik=int(LIMIT_PER_CIK),
            window_days_floor=2,
            debug=True,
        )

        print(f"Fetched {len(filings)} filings (submissions universe batch)")

        counts = {
            "total": 0,
            "skipped_seen": 0,
            "no_text": 0,
            "no_reverse_lang": 0,
            "reverse_lang": 0,
            "rejected_by_policy": 0,
            "accepted": 0,
            "missing_effective_date": 0,
        }

        results: List[dict] = []
        rejections: List[dict] = []

        def reject(filing, stage: str, reason: str, extra: dict | None = None):
            acc_no_dashes = filing.accession.replace("-", "")
            default_url = f"https://www.sec.gov/Archives/edgar/data/{int(filing.cik)}/{acc_no_dashes}/{filing.accession}-index.html"
            filing_url = getattr(filing, "url", None) or getattr(filing, "link", None) or default_url

            # IMPORTANT: keep a stable set of fields across ALL rejection records
            rec = {
                "accession": filing.accession,
                "company": filing.company,
                "cik": filing.cik,
                "form": filing.form,
                "filed_at": filing.filed_at.isoformat() if getattr(filing, "filed_at", None) else None,
                "filing_url": filing_url,
                "stage": stage,
                "reason": reason,

                # --- optional fields (always present; None when unknown) ---
                "ticker": None,
                "exchange": None,
                "effective_date": None,
                "ratio_display": None,
                "ratio_new": None,
                "ratio_old": None,
                "rounding_policy": None,
                "event_dt": None,
                "event_age_hours": None,
            }

            if extra:
                # extra may include any of the optional fields above; that’s fine now
                rec.update(extra)

            rejections.append(rec)

        for filing in filings:
            counts["total"] += 1

            now_et = datetime.now(ZoneInfo("America/New_York"))
            filing_time = filing.filed_at
            if filing_time.tzinfo is None:
                filing_time = filing_time.replace(tzinfo=ZoneInfo("America/New_York"))
            else:
                filing_time = filing_time.astimezone(ZoneInfo("America/New_York"))
            age_hours = (now_et - filing_time).total_seconds() / 3600

            if age_hours > WINDOW_HOURS:
                counts["rejected_by_policy"] += 1
                reject(filing, "age", f"Old filing: {age_hours:.1f}h > {WINDOW_HOURS}h", {"event_age_hours": round(age_hours, 2)})
                continue

            if not FORCE_REPROCESS and filing.accession in self.seen:
                counts["skipped_seen"] += 1
                reject(filing, "seen", "Already processed (seen accession)")
                continue

            text = edgar.fetch_filing_text(
                filing, self.filing_cache, self.session, USER_AGENT
            )
            if not text:
                counts["no_text"] += 1
                reject(filing, "no_text", "No filing text returned")
                continue

            if parse.is_delisting_notice_only(text):
                counts["rejected_by_policy"] += 1
                continue
            # 1) Reverse-split trigger
            has_reverse = parse.contains_reverse_split_language(text)
            if not has_reverse:
                counts["no_reverse_lang"] += 1
                reject(filing, "reverse_lang", "No reverse split language detected")
                continue

            # 2) NEW: Require a fresh event date (Period of Report / earliest event reported)
            event_dt = parse.extract_event_reported_datetime(text)

            if event_dt is None:
                # STRICT: if it fired "reverse split language" but we can't locate an event date,
                # we do NOT allow it through (prevents stale/boilerplate false positives).
                counts["rejected_by_policy"] += 1
                reject(filing, "event_dt", "Reverse language present but no event date parsed")
                continue

            event_age_hours = (now_et - event_dt).total_seconds() / 3600
            if event_age_hours > WINDOW_HOURS:
                counts["rejected_by_policy"] += 1
                reject(filing, "event_dt", f"Stale event_dt: {event_age_hours:.1f}h > {WINDOW_HOURS}h",
                {"event_dt": event_dt.isoformat(), "event_age_hours": round(event_age_hours, 2)})
                continue

            counts["reverse_lang"] += 1


            if not FORCE_REPROCESS:
                self.seen.add(filing.accession)

            extraction = parse.extract_details(text, filed_at=filing.filed_at)
            

            meta = self.tickers.lookup(filing.cik)
            ticker_map = (meta.get("ticker") or "").upper().strip()
            exchange_map = (meta.get("exchange") or "").upper().strip()
            exchange = exchange_map  # working var
            title = meta.get("title", filing.company)

            ticker = ticker_map

            tkr2, exch2 = parse.extract_common_ticker_exchange(text)
            if tkr2:
                ticker = tkr2
                # FIX: normalize exchange even if exch2 is missing
                exchange = (exch2 or exchange_map).upper().strip()
            else:
                tmp_info = filters.SecurityInfo(ticker=ticker_map, exchange=exchange_map, title=title)
                if filters.is_non_common_security(tmp_info):
                    derived = derive_common_ticker_from_map(ticker_map)
                    if derived:
                        ticker = derived
                        exchange = exchange_map

            sec_info = filters.SecurityInfo(ticker=ticker, exchange=exchange, title=title)

            rejection = filters.summarize_rejection(
                text=text,
                meta=sec_info,
                policy=extraction.rounding_policy,
                price=None,
                ratio_new=extraction.ratio_new,
                ratio_old=extraction.ratio_old,
            )

            today = datetime.now(ZoneInfo("America/New_York")).date()
            # --- Effective date must exist ---
            if extraction.effective_date is None:
                counts["rejected_by_policy"] += 1
                reject(
                    filing,
                    stage="effective_date",
                    reason="Effective date not found",
                    extra={
                        "rounding_policy": extraction.rounding_policy,
                        "ratio_new": extraction.ratio_new,
                        "ratio_old": extraction.ratio_old,
                    },
                )
                continue

            # --- Effective date must not be in the past ---
            if extraction.effective_date.date() < today:
                counts["rejected_by_policy"] += 1
                reject(
                    filing,
                    stage="effective_date",
                    reason=f"Effective date already passed ({extraction.effective_date.date().isoformat()})",
                    extra={
                        "effective_date": extraction.effective_date.isoformat(),
                        "rounding_policy": extraction.rounding_policy,
                        "ratio_new": extraction.ratio_new,
                        "ratio_old": extraction.ratio_old,
                    },
                )
                continue

            # --- Reject explicit ROUND_DOWN policies ---
            if extraction.rounding_policy == parse.ROUND_DOWN:
                counts["rejected_by_policy"] += 1
                reject(
                    filing,
                    stage="rounding_policy",
                    reason="Explicit ROUND_DOWN rounding policy",
                    extra={
                        "effective_date": extraction.effective_date.isoformat(),
                        "ratio_new": extraction.ratio_new,
                        "ratio_old": extraction.ratio_old,
                    },
                )
                continue


            # Build an SEC filing index URL (works even if the Filing object doesn't store a URL)
            acc_no_dashes = filing.accession.replace("-", "")
            default_url = f"https://www.sec.gov/Archives/edgar/data/{int(filing.cik)}/{acc_no_dashes}/{filing.accession}-index.html"
            filing_url = getattr(filing, "url", None) or getattr(filing, "link", None) or default_url

            record = {
                "accession": filing.accession,
                "company": filing.company,
                "cik": filing.cik,
                "ticker": ticker,
                "exchange": exchange,
                "form": filing.form,
                "filed_at": filing_time.isoformat(),
                "filing_url": filing_url,   # <-- NEW
                "effective_date": extraction.effective_date.isoformat() if extraction.effective_date else None,
                "ratio_display": f"{extraction.ratio_new}-for-{extraction.ratio_old}",
                "ratio_new": extraction.ratio_new,
                "ratio_old": extraction.ratio_old,
                "rounding_policy": extraction.rounding_policy,
                "price": None,
                "potential_profit": None,
                "rejection_reason": rejection,
                "event_dt": event_dt.isoformat() if event_dt else None,
                "event_age_hours": round(event_age_hours, 2) if event_dt else None,
            }

            if rejection is None:
                results.append(record)
                counts["accepted"] += 1
            else:
                counts["rejected_by_policy"] += 1
                reject(filing, "filters", rejection, {
                    "ticker": ticker,
                    "exchange": exchange,
                    "effective_date": record["effective_date"],
                    "ratio_display": record["ratio_display"],
                    "rounding_policy": record["rounding_policy"],
                    "event_dt": record["event_dt"],
                })

        self.filing_cache.save()
        self.seen.save()

        self._enrich_with_stooq(results)

        def dedupe_events(records: list[dict]) -> list[dict]:
            best = {}

            for r in records:
                key = (
                    r.get("ticker"),
                    r.get("ratio_new"),
                    r.get("ratio_old"),
                    r.get("rounding_policy"),
                )

                cur = best.get(key)
                if cur is None:
                    best[key] = r
                    continue

                # Prefer a record that has an effective_date
                r_eff = r.get("effective_date")
                c_eff = cur.get("effective_date")
                if (not c_eff) and r_eff:
                    best[key] = r
                    continue
                if c_eff and (not r_eff):
                    continue

                # Otherwise keep latest filed_at
                if (r.get("filed_at") or "") > (cur.get("filed_at") or ""):
                    best[key] = r

            return list(best.values())


        results = dedupe_events(results)

        alert.write_json(RESULTS_JSON, results)
        alert.write_csv(RESULTS_CSV, results)

        alert.write_json(REJECTIONS_JSON, rejections)
        alert.write_csv(REJECTIONS_CSV, rejections)
        print(f"Wrote {len(rejections)} rejections to {REJECTIONS_JSON} and {REJECTIONS_CSV}")


        print("Filter stats:", counts)
        return results

    def _enrich_with_stooq(self, results: List[dict]) -> None:
        """Fetch prices for accepted results and compute potential profit."""
        if not results:
            return

        for record in results:
            ticker = record.get("ticker")
            ratio_new = record.get("ratio_new")
            ratio_old = record.get("ratio_old")

            px = price.fetch_price_with_fallback(ticker, self.price_cache, self.session)
            record["price"] = px

            potential = None
            if px is not None and ratio_new and ratio_old:
                potential = round(px * (ratio_old / ratio_new) - px, 4)
            record["potential_profit"] = potential

        self.price_cache.save()
        self._print_profit_estimates(results)

    def _print_profit_estimates(self, results: List[dict]) -> None:
        print("\nStooq potential profit estimates:")
        for record in results:
            ticker = record.get("ticker")
            profit = record.get("potential_profit")
            price_val = record.get("price")
            ratio_display = record.get("ratio_display") or "n/a"
            ratio_new = record.get("ratio_new")
            ratio_old = record.get("ratio_old")

            missing_fields = []
            if price_val is None:
                missing_fields.append("price")
            if not ratio_new or not ratio_old:
                missing_fields.append("ratio")

            if missing_fields:
                missing_display = "/".join(missing_fields)
                print(f" - {ticker}: missing {missing_display} data")
                continue

            print(
                f" - {ticker}: pre-split price ${price_val:.4f} -> potential profit ${profit:.4f} ({ratio_display})"
            )


def maybe_email(results: List[dict]) -> None:
    sender = os.environ.get("ALERT_SENDER_EMAIL")
    pwd = os.environ.get("ALERT_SENDER_APP_PWD")
    recipients_raw = os.environ.get("ALERT_RECIPIENTS", "")
    if sender and pwd and recipients_raw:
        recipients = [r.strip() for r in recipients_raw.split(",") if r.strip()]
        alert.send_email(results, sender, pwd, recipients)

import re
import requests
from datetime import datetime
from zoneinfo import ZoneInfo

def _fetch_sec_index_and_find_primary_txt(index_url: str, session: requests.Session) -> str:
    """
    Given an SEC '-index.html' page, find the primary filing .txt URL.
    This avoids quotemedia/3rd party HTML quirks and uses SEC source of truth.
    """
    r = session.get(index_url, timeout=30)
    r.raise_for_status()
    html = r.text

    # Common pattern in SEC index pages: a link to the complete submission text file ending in .txt
    # Example: /Archives/edgar/data/.../000xxxxxxx-xx-xxxxxx.txt
    m = re.search(r'href="([^"]+\.txt)"', html, flags=re.IGNORECASE)
    if not m:
        # fallback: sometimes the .txt is in a different attribute formatting
        m = re.search(r'(/Archives/edgar/data/[^"\s]+\.txt)', html, flags=re.IGNORECASE)
    if not m:
        raise RuntimeError("Could not find primary .txt filing link on index page")

    href = m.group(1)
    if href.startswith("/"):
        return "https://www.sec.gov" + href
    if href.startswith("https://"):
        return href
    # If relative without leading slash (rare)
    return "https://www.sec.gov/" + href.lstrip("/")

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def explain_would_include(
    text: str,
    extraction,
    event_is_fresh: bool,
    filed_at=None,
    now_et: datetime | None = None,
) -> tuple[bool, list[str]]:
    """
    Mirrors production gating and returns human-readable reasons.
    """
    if now_et is None:
        now_et = datetime.now(ZoneInfo("America/New_York"))

    reasons: list[str] = []

    # Gate 1: delisting-only
    if parse.is_delisting_notice_only(text):
        reasons.append("FAIL: delisting notice only")

    # Gate 2: reverse split language
    if not parse.contains_reverse_split_language(text):
        reasons.append("FAIL: no reverse-split language detected")

    # Gate 3: freshness (your event-based freshness flag)
    if not event_is_fresh:
        reasons.append("FAIL: event is not fresh (outside WINDOW_HOURS)")

    # Gate 4: effective date exists
    if extraction.effective_date is None:
        reasons.append("FAIL: effective_date not found")

    # Optional: these are *additional* gates you apply later, so include them too
    if extraction.rounding_policy == parse.ROUND_DOWN:
        reasons.append("FAIL: explicit ROUND_DOWN policy")

    # Optional: effective date already passed (you check this later)
    if extraction.effective_date is not None:
        today = now_et.date()
        if extraction.effective_date.date() < today:
            reasons.append(f"FAIL: effective_date already passed ({extraction.effective_date.date().isoformat()})")

    would_include = (len(reasons) == 0)

    if would_include:
        reasons.append("PASS: meets all inclusion gates")

    return would_include, reasons


def debug_two_filings(index_urls: list[str], WINDOW_HOURS: int = 84) -> None:
    runner = Runner()
    print("\n========== DEBUG TWO FILINGS ==========")
    for idx_url in index_urls:
        print("\n--------------------------------------")
        print("INDEX:", idx_url)

        txt_url = _fetch_sec_index_and_find_primary_txt(idx_url, runner.session)
        print("PRIMARY TXT:", txt_url)

        resp = runner.session.get(txt_url, timeout=30)
        resp.raise_for_status()
        text = resp.text

        now_et = datetime.now(ZoneInfo("America/New_York"))

        # --- Event freshness gate ---
        event_dt = parse.extract_event_reported_datetime(text)
        event_age_hours = (
            (now_et - event_dt).total_seconds() / 3600
            if event_dt else None
        )
        event_is_fresh = (
            event_age_hours is not None and event_age_hours <= WINDOW_HOURS
        )

        print("event_dt:", event_dt.isoformat() if event_dt else None)
        print("event_age_hours:", round(event_age_hours, 2) if event_age_hours else None)
        print("event_is_fresh:", event_is_fresh)

        print("TXT length:", len(text))

        print("contains_reverse_split_language:", parse.contains_reverse_split_language(text))
        print("is_delisting_notice_only:", parse.is_delisting_notice_only(text))

        # ---- Extraction (MUST happen before include decision) ----
        filed_at_guess = now_et
        extraction = parse.extract_details(text, filed_at=filed_at_guess)

        print("\n[EXTRACTED]")
        print("  ratio_new:", extraction.ratio_new)
        print("  ratio_old:", extraction.ratio_old)
        print("  ratio_display:", f"{extraction.ratio_new}-for-{extraction.ratio_old}")
        print("  effective_date:", extraction.effective_date)
        print("  rounding_policy:", extraction.rounding_policy)

        # ---- FINAL POLICY DECISION (matches production) ----
        would_include = (
            not parse.is_delisting_notice_only(text)
            and parse.contains_reverse_split_language(text)
            and event_is_fresh
            and extraction.effective_date is not None
        )

        would_include, reasons = explain_would_include(
            text=text,
            extraction=extraction,
            event_is_fresh=event_is_fresh,
            filed_at=filed_at_guess,
            now_et=now_et,
        )

        print("WOULD_INCLUDE (policy):", would_include)
        for r in reasons:
            print("  -", r)

        # Context helpers unchanged
        def show_context(pattern: str, label: str, window: int = 220):
            m = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if not m:
                print(f"\n[CTX] {label}: NOT FOUND  | pattern={pattern}")
                return
            s, e = m.start(), m.end()
            lo = max(0, s - window)
            hi = min(len(text), e + window)
            print(f"\n[CTX] {label}: FOUND")
            print(text[lo:hi])

        show_context(
            r"\b\d{1,3}(?:,\d{3})*\s*[-–]\s*for\s*[-–]\s*\d{1,3}(?:,\d{3})*\b",
            "hyphenated ratio X-for-Y",
        )
        show_context(
            r"between\s+one[-\s]*for[-\s]*two.*one[-\s]*for[-\s]*ten",
            "ratio RANGE language (authorization)",
        )
        show_context(
            r"implemented\s+effective|will\s+be\s+effective|effective\s+date|market\s+effective\s+date|begin\s+trading\s+on\s+a\s+split-adjusted\s+basis",
            "effective-date language",
        )
        show_context(
            r"fractional\s+share|no\s+fractional\s+shares|rounded\s+up|cash\s+in\s+lieu|paid\s+in\s+cash",
            "fractional/rounding language",
        )



if __name__ == "__main__":
    # index_urls = [
    #     "https://www.sec.gov/Archives/edgar/data/868278/000149315225028317/0001493152-25-028317-index.html",
    #     "https://www.sec.gov/Archives/edgar/data/1624512/000162828025058104/0001628280-25-058104-index.html"
    # ]
    # debug_two_filings(index_urls)

    runner = Runner()
    results = runner.run()
    maybe_email(results)
    print(f"Wrote {len(results)} results to {RESULTS_JSON}")
