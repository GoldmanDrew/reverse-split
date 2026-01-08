# reverse-split

Scanner for U.S. reverse stock split announcements that explicitly round fractional shares up to whole shares.

## What it does

- Pulls fresh SEC filings (8-K, DEF 14A/PRE 14A, S-1/F-1 and amendments) from EDGAR.
- Fast filter: keeps only filings that mention reverse splits and fractional-share language.
- Deep parse: extracts ratios, effective dates, and rounding policies; classifies ROUND_UP vs. cash-in-lieu.
- Filters out ADRs/ETFs/Canadian issuers and drops splits that would still be sub-$1.00 post-ratio.
- Caches filings/prices/ticker metadata so the same accession is never fetched twice.
- Writes JSON/CSV results and can optionally email a digest.

## Repository layout

```
reverse-split/
  src/
    edgar.py     # EDGAR fetch helpers + caching
    parse.py     # keyword filters, ratio/date/rounding extraction
    filters.py   # ADR/ETF/Canada + rounding + price threshold filters
    price.py     # price fetch with day-level cache
    alert.py     # email + result writers
  data/
    .keep        # placeholder so the directory is tracked
  run.py         # entrypoint orchestrating the two-stage pipeline
  requirements.txt
  .github/workflows/scan.yml
```

## Running locally

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set environment variables (recommended). The SEC requires a descriptive user agent **with your contact information**; otherwise requests will be blocked with 403 responses:
   ```bash
   export SEC_USER_AGENT="Your Name contact@example.com reverse-split-monitor/0.1"
   export WINDOW_HOURS=72                       # lookback window for fresh filings
   export ALERT_SENDER_EMAIL="you@gmail.com"    # optional, required for email
   export ALERT_SENDER_APP_PWD="app-password"   # optional, required for email
   export ALERT_RECIPIENTS="first@ex.com,second@ex.com"
   ```
3. Run the scanner:
   ```bash
   python run.py
   ```
Results are written to `data/results.json` (and CSV) and cached filings/prices are stored under `data/`.

### Automated trading (IBKR)

To automatically short an equal-weight basket of flagged candidates through IBKR:

1. Install the optional dependency: `pip install -r requirements.txt` (includes `ib-insync`).
2. Run IBKR Gateway or TWS and expose the API.
3. Set trading environment variables:
   ```bash
   export ENABLE_IBKR_TRADING=1
   export IBKR_HOST=127.0.0.1
   export IBKR_PORT=7497              # or paper 7497/live 7496
   export IBKR_CLIENT_ID=18
   export IBKR_TOTAL_NOTIONAL=25000   # total dollar notional to split equally
   export MAX_BORROW_RATE=0.25        # skip names with fee rate above 25%
   export COMPLIANCE_PRICE=1.15       # drop if 2 closes above this level
   export COMPLIANCE_DAYS=2
   ```
4. Execute `python run.py`. After the screener runs, the bot will:
   - Drop ADR/ETF/Canadian issuers (existing filters).
   - Skip names that already closed above $1.00/$1.15 for two sessions.
   - Skip names with borrow fee rates above your threshold.
   - Submit Adaptive market short orders sized equally by notional.

## Automation (GitHub Actions)

`.github/workflows/scan.yml` schedules runs twice per weekday. To enable alerts, add the following repository secrets:

- `SEC_USER_AGENT` – required by the SEC for scripted access (e.g., `Your Name contact@example.com`).
- `ALERT_SENDER_EMAIL`, `ALERT_SENDER_APP_PWD`, `ALERT_RECIPIENTS` for Gmail-based email delivery.

The workflow installs dependencies, executes `python run.py`, and uploads `data/results.json` as an artifact.

## Why this approach

- SEC filings + exhibits are the source of truth for reverse split terms and rounding policies.
- Two-stage pipeline keeps it fast: keyword prefilter, then detailed extraction only for candidates.
- Caching and accession dedupe prevent repeated downloads and price lookups.
- Filters focus on actionable events: ROUND_UP language, near-term effective dates, and post-split price sanity.
