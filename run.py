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

WINDOW_HOURS = 72

USER_AGENT = edgar.USER_AGENT

FORCE_REPROCESS = 0

LIMIT_PER_CIK = 5

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
        }

        results: List[dict] = []

        for filing in filings:
            counts["total"] += 1

            if not FORCE_REPROCESS and filing.accession in self.seen:
                counts["skipped_seen"] += 1
                continue

            text = edgar.fetch_filing_text(
                filing, self.filing_cache, self.session, USER_AGENT
            )
            if not text:
                counts["no_text"] += 1
                continue

            if parse.is_delisting_notice_only(text):
                counts["rejected_by_policy"] += 1
                continue

            if not parse.contains_reverse_split_language(text):
                counts["no_reverse_lang"] += 1
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

            if extraction.effective_date is not None and extraction.effective_date.date() < today:
                counts["rejected_by_policy"] += 1
                print(f"Effective date already passed ({extraction.effective_date.date().isoformat()})")
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
                "filed_at": filing.filed_at.isoformat(),
                "filing_url": filing_url,   # <-- NEW
                "effective_date": extraction.effective_date.isoformat() if extraction.effective_date else None,
                "ratio_display": f"{extraction.ratio_new}-for-{extraction.ratio_old}",
                "ratio_new": extraction.ratio_new,
                "ratio_old": extraction.ratio_old,
                "rounding_policy": extraction.rounding_policy,
                "price": None,
                "potential_profit": None,
                "rejection_reason": rejection,
            }

            
            if rejection is None:
                results.append(record)
                counts["accepted"] += 1
            else:
                counts["rejected_by_policy"] += 1
                print(record["rejection_reason"])

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
                potential = round(px * (ratio_old / ratio_new), 4)
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


if __name__ == "__main__":
    runner = Runner()
    results = runner.run()
    maybe_email(results)
    print(f"Wrote {len(results)} results to {RESULTS_JSON}")
