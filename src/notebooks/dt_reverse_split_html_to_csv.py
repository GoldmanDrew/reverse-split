import json
import pandas as pd
from bs4 import BeautifulSoup

INPUT_FILE = "getReverseSplit_response.json"
OUTPUT_FILE = "dilutiontracker_reverse_splits.csv"

# Load response
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)

html = data.get("__html")
if not html:
    raise SystemExit("❌ '__html' field not found")

# Parse HTML
soup = BeautifulSoup(html, "html.parser")
table = soup.find("table")

if table is None:
    raise SystemExit("❌ No table found in HTML")

# Extract rows
rows = []
headers = []

for i, tr in enumerate(table.find_all("tr")):
    cells = [c.get_text(strip=True) for c in tr.find_all(["td", "th"])]
    if not cells:
        continue

    if i == 0:
        headers = cells
    else:
        rows.append(cells)

# Build DataFrame
df = pd.DataFrame(rows, columns=headers)

# Normalize columns
df.columns = (
    df.columns
      .str.lower()
      .str.replace(" ", "_")
      .str.replace(r"[()]", "", regex=True)
)

# Save
df.to_csv(OUTPUT_FILE, index=False)

print(f"✅ Exported {len(df)} rows → {OUTPUT_FILE}")
