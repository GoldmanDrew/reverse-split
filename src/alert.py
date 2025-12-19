import json
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import List


def render_email_body(results: List[dict]) -> str:
    lines = ["Reverse split opportunities (round-up only):", ""]
    for item in results:
        lines.append(f"{item['ticker']} ({item['company']}) - {item['form']} {item['accession']}")
        lines.append(f"  Ratio: {item.get('ratio_display', 'n/a')} | Effective: {item.get('effective_date', 'n/a')}")
        lines.append(f"  Rounding: {item.get('rounding_policy')} | Exchange: {item.get('exchange', '')}")
        lines.append(f"  Filing: {item.get('filing_url', 'n/a')}")

        potential_profit = item.get("potential_profit")
        price = item.get("price")
        if potential_profit is not None and price is not None:
            lines.append(
                f"  Potential profit: ${potential_profit:.4f} (pre-split price ${price:.4f})"
            )
        else:
            lines.append("  Potential profit: n/a")
        lines.append("")
    return "\n".join(lines)


def send_email(results: List[dict], sender: str, password: str, recipients: List[str]) -> None:
    if not results:
        return
    msg = EmailMessage()
    msg["Subject"] = "Reverse split round-up alerts"
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)
    msg.set_content(render_email_body(results))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(sender, password)
        smtp.send_message(msg)


def write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str))


def write_csv(path: Path, data: List[dict]) -> None:
    if not data:
        return
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(data[0].keys())
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
