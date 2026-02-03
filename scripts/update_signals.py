import csv
import io
import json
from datetime import datetime, timezone
from urllib.request import Request, urlopen

COMMISSION_CSV_URL = "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/compliance-actions/search/export"
OUTPUT_PATH = "docs/signals.json"

def fetch_bytes(url: str) -> bytes:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=60) as r:
        return r.read()

def clean(s: str) -> str:
    return (s or "").strip()

def pick(row: dict, *keys: str) -> str:
    for k in keys:
        if k in row and row[k]:
            return clean(row[k])
    return ""

def risk_from_type(action_type: str) -> str:
    t = (action_type or "").lower()
    if any(x in t for x in ["banning", "revocation", "cancel", "prohibition", "injunction", "civil penalty"]):
        return "HIGH"
    if any(x in t for x in ["enforceable undertaking", "compliance notice", "notice", "direction"]):
        return "MED"
    return "LOW"

def main():
    raw = fetch_bytes(COMMISSION_CSV_URL)
    text = raw.decode("utf-8", errors="ignore")

    reader = csv.DictReader(io.StringIO(text))

    signals = []
    for row in reader:
        action_type = pick(row, "Type", "Action type", "Compliance action", "Compliance Action", "Action")
        name = pick(row, "Name", "Provider", "Provider name", "Registered provider", "Entity")
        state = pick(row, "State", "Jurisdiction")
        effective = pick(row, "Effective", "Effective date", "Start date", "Commencement date", "Date effective")
        end = pick(row, "End", "End date", "Expiry date", "Cease date")

        if not (action_type or name):
            continue

        signals.append({
            "risk": risk_from_type(action_type),
            "type": action_type or "Compliance action",
            "name": name or "Unnamed entity",
            "state": state or "",
            "effective": effective or "",
            "end": end or ""
        })

    data = {
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "count": len(signals),
        "signals": signals[:500]
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"signals.json updated: {len(signals)} rows")

if __name__ == "__main__":
    main()
