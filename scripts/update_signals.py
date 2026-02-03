import json
from datetime import datetime, timezone
from urllib.request import urlopen, Request

COMMISSION_CSV_URL = "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/compliance-actions/search/export"

OUTPUT_PATH = "docs/signals.json"

def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="ignore")

def main():
    try:
        csv_text = fetch_text(COMMISSION_CSV_URL)
    except Exception as e:
        print("Fetch failed:", e)
        return

    data = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "raw_preview": csv_text[:1000]
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print("signals.json updated")

if __name__ == "__main__":
    main()
