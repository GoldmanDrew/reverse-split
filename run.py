#!/usr/bin/env python3
import os
from datetime import datetime
from pathlib import Path
from typing import List

import requests

from src import alert, edgar, filters, parse, price

DATA_DIR = Path("data")
RESULTS_JSON = DATA_DIR / "results.json"
RESULTS_CSV = DATA_DIR / "results.csv"
CACHE_FILINGS = DATA_DIR / "cache_filings.json"
SEEN_ACCESSIONS = DATA_DIR / "seen_accessions.json"
TICKER_MAP_PATH = DATA_DIR / "ticker_map.json"
PRICE_CACHE_PATH = Path("price_cache.json")

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
        self.price_cache = price.PriceCache(PRICE_CACHE_PATH)

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

            rejection = filters.summarize_rejection(
                text=text,
                meta=sec_info,
                policy=extraction.rounding_policy,
                price=None,
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
                "ratio_new": extraction.ratio_new,
                "ratio_old": extraction.ratio_old,
                "rounding_policy": extraction.rounding_policy,
                "price": None,
                "potential_profit": None,
                "rejection_reason": rejection,
            }

            if filing.cik == "0002088749":
                ctx = parse.extract_reverse_split_context(text)
                print("---- Beneficient reverse-split context ----")
                print(ctx[:4000])  # show first 4k chars of the scoped section
                print("---- end ----")
                extraction_dbg = parse.extract_details(text, filed_at=filing.filed_at)
                print("DEBUG policy:", extraction_dbg.rounding_policy)
                tl = text.lower()
                print("has 'reverse stock split'?:", "reverse stock split" in tl)
                print("has 'additional share'?:", "additional share" in tl)
                print("has 'in lieu of a fractional share'?:", "in lieu of a fractional share" in tl)
                print("has 'cash in lieu'?:", "cash in lieu" in tl)


            if rejection is None:
                results.append(record)
                counts["accepted"] += 1
            else:
                counts["rejected_by_policy"] += 1
                print(record['rejection_reason'])

        self.filing_cache.save()
        self.seen.save()
        self._enrich_with_stooq(results)
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
