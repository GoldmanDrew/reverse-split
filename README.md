# reverse-split

A small helper script that scans Google News for reverse split announcements and flags situations where fractional shares might be rounded up to whole shares ("free round up" scenarios). It searches broadly, filters out clear false positives, applies price/ratio sanity checks, and emails a digest of any newly detected opportunities.

## Running locally

1. Create a Python environment with the dependencies:
   ```bash
   pip install -r requirements.txt
   ```
2. Set the email credentials (Gmail with an App Password) as environment variables:
   ```bash
   export ALERT_SENDER_EMAIL="you@example.com"
   export ALERT_SENDER_APP_PWD="app-password"
   ```
3. Run the scanner:
   ```bash
   python reverse_split.py
   ```

## Automation (GitHub Actions)

A scheduled GitHub Actions workflow runs the scanner every day at **9:00 UTC** and sends the email alert to the configured recipients. To enable it:

1. Add the repository secrets `ALERT_SENDER_EMAIL` and `ALERT_SENDER_APP_PWD` with your Gmail address and app password.
2. Ensure the default branch contains the workflow at `.github/workflows/daily-email.yml`.
3. Keep `reverse_split.py` and `requirements.txt` in the repository root so the workflow can install dependencies and run the scanner.

The workflow installs dependencies, executes the scanner, and sends the daily digest email when any new qualifying items are found.
