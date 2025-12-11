# reverse-split

A small helper script that scans Google News for reverse split announcements and flags situations where fractional shares might be rounded up to whole shares ("free round up" scenarios). It searches broadly, filters out clear false positives, applies price/ratio sanity checks, and looks for effective dates occurring within the next five days. The date check now combines:

- Month/day parsing in feed snippets and linked articles
- Day-of-week-only clues ("effective Monday at the open") mapped to the next matching date
- A recency fallback that keeps reverse-split articles published within the window even if no explicit effective date is present

After filtering, it emails a digest of any newly detected opportunities.

## Running locally

1. Create a Python environment with the dependencies (news feed parsing, HTML scraping for dates, price lookups):
   ```bash
   pip install -r requirements.txt
   ```
2. Set the email credentials (Gmail with an App Password) as environment variables. The sender defaults to `werdnamdlog01@gmail.com`, and the recipient list is hard-coded in `reverse_split.py`:
   ```bash
   # Required: Gmail app password for werdnamdlog01@gmail.com
   export ALERT_SENDER_APP_PWD="app-password"

   # Optional: override the sender address
   export ALERT_SENDER_EMAIL="alternate@gmail.com"
   ```
3. Run the scanner:
   ```bash
   python reverse_split.py
   ```
   Results that pass all filters for the most recent run are written to `reverse_split_results.json` in the repository root so you can inspect them even if no email is sent.

## Automation (GitHub Actions)

A scheduled GitHub Actions workflow runs the scanner every day at **9:00 UTC** and sends the email alert to the configured recipients. To enable it:

1. Add the repository secret `ALERT_SENDER_APP_PWD` with the Gmail app password for `werdnamdlog01@gmail.com`. If you need to override the sender address, also add the optional `ALERT_SENDER_EMAIL` secret.
2. Ensure the default branch contains the workflow at `.github/workflows/daily-email.yml`.
3. Keep `reverse_split.py` and `requirements.txt` in the repository root so the workflow can install dependencies and run the scanner.

The workflow installs dependencies, executes the scanner, and sends the daily digest email when any new qualifying items are found.
