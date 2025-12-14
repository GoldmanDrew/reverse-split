#!/usr/bin/env python3
import os
from datetime import datetime
from pathlib import Path
from typing import List

import requests

from src import alert, edgar, filters, parse

DATA_DIR = Path("data")
RESULTS_JSON = DATA_DIR / "results.json"
RESULTS_CSV = DATA_DIR / "results.csv"
CACHE_FILINGS = DATA_DIR / "cache_filings.json"
SEEN_ACCESSIONS = DATA_DIR / "seen_accessions.json"
TICKER_MAP_PATH = DATA_DIR / "ticker_map.json"

WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "72"))
WINDOW_HOURS = 720
USER_AGENT = os.environ.get("SEC_USER_AGENT", edgar.USER_AGENT)

# If set to "1", ignore seen_accessions.json and reprocess filings in-window.
FORCE_REPROCESS = os.environ.get("FORCE_REPROCESS", "0") == "1"


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

    def run(self) -> List[dict]:
        filings = edgar.fetch_recent_filings(edgar.FORMS_OF_INTEREST, WINDOW_HOURS, self.session, USER_AGENT)
        print(f"Fetched {len(filings)} filings")

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

            # IMPORTANT: if a filing was previously rejected due to a transient issue
            # (e.g., Yahoo price lookup), you don't want it to be permanently skipped.
            # Two safeguards:
            #   1) FORCE_REPROCESS=1 ignores seen.
            #   2) We only add to seen AFTER we confirm it's a reverse-split candidate.
            if not FORCE_REPROCESS and filing.accession in self.seen:
                counts["skipped_seen"] += 1
                continue

            text = edgar.fetch_filing_text(filing, self.filing_cache, self.session, USER_AGENT)
            if not text:
                counts["no_text"] += 1
                continue

            if not parse.contains_reverse_split_language(text):
                counts["no_reverse_lang"] += 1
                continue

            counts["reverse_lang"] += 1

            # Mark as seen ONLY once we know it matched the reverse-split detector.
            # This avoids permanently skipping filings that were rejected due to
            # later-stage filters you might change in the future.
            if not FORCE_REPROCESS:
                self.seen.add(filing.accession)

            extraction = parse.extract_details(text, filed_at=filing.filed_at)
            meta = self.tickers.lookup(filing.cik)
            ticker = meta.get("ticker") or filing.cik
            exchange = meta.get("exchange", "")
            title = meta.get("title", filing.company)
            sec_info = filters.SecurityInfo(ticker=ticker, exchange=exchange, title=title)

            # Price checks removed entirely. Keep a placeholder for schema stability.
            px = None

            rejection = filters.summarize_rejection(
                text=text,
                meta=sec_info,
                policy=extraction.rounding_policy,
                price=px,
                ratio_new=extraction.ratio_new,
                ratio_old=extraction.ratio_old,
            )

            record = {
                "accession": filing.accession,
                "company": filing.company,
                "cik": filing.cik,
                "ticker": ticker,
                "exchange": exchange,
                "form": filing.form,
                "filed_at": filing.filed_at.isoformat(),
                "effective_date": extraction.effective_date.isoformat() if extraction.effective_date else None,
                "ratio_display": f"{extraction.ratio_new}-for-{extraction.ratio_old}" if extraction.ratio_new else None,
                "rounding_policy": extraction.rounding_policy,
                "price": px,
                "rejection_reason": rejection,
            }
            print(
                "Beneficient in fetched filings:",
                any(f.cik == "0002088749" for f in filings)
            )
            if filing.cik == "0002088749":
                print("DEBUG Beneficient:", {
                    "accession": filing.accession,
                    "form": filing.form,
                    "filed_at": filing.filed_at,
                })
                print("DEBUG policy before rejection:", extraction.rounding_policy)
                tl = text.lower()
                i = tl.find("fraction")
                print("---- Beneficient fractional context ----")
                print(text[max(0, i-500): i+800])
                print("---- end ----")

            if rejection is None:
                results.append(record)
                counts["accepted"] += 1
            else:
                counts["rejected_by_policy"] += 1
                print(record['rejection_reason'])

        self.filing_cache.save()
        self.seen.save()
        alert.write_json(RESULTS_JSON, results)
        alert.write_csv(RESULTS_CSV, results)

        print("Filter stats:", counts)
        return results


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
