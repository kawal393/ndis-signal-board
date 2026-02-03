import csv, json, os, re
from datetime import datetime, timezone
from urllib.request import urlopen, Request

COMMISSION_CSV_URL = "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/compliance-actions/search/export"

# Optional RSS feeds (you can add later)
RSS_FEEDS = [
  # Example: "https://www.google.com/alerts/feeds/XXXXX"
]

def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="ignore")

def parse_commission_csv(csv_text: str):
    rows = []
    reader = csv.DictReader(csv_text.splitlines())
    for row in reader:
        # Keep only relevant fields; CSV headers can change, so be defensive
        item = {
            "changeType": row.get("Type") or "Compliance",
            "date_effective": row.get("Date effective from") or row.get("Date effective from (AEST/AEDT)") or "",
            "date_end": row.get("Date no longer in effect") or "",
            "name": row.get("Name") or "",
            "state": row.get("State") or row.get("Jurisdiction") or "National",
        }
        rows.append(item)
    return rows

def gemini_summarize(api_key: str, title: str, raw: str) -> dict:
    # Minimal, robust Gemini call via REST (no SDK needed)
    import urllib.request

    endpoint = "https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key=" + api_key
    prompt = f"""
You are an NDIS market signals analyst. Output ONLY valid JSON.

Schema:
{{
  "changeType":"Compliance|Banning|Registration|Price|Guideline|Other",
  "affectedService":"SIL|Plan Management|Support Coordination|Core Supports|All",
  "affectedArea":"National|VIC|NSW|QLD|SA|WA|TAS|NT|ACT",
  "impactScore":0-100,
  "why":"one short sentence",
  "actionForProviders":"one short sentence"
}}

TITLE: {title}
RAW: {raw}
"""
    body = {
      "contents": [{"parts": [{"text": prompt}]}],
      "generationConfig": {"temperature": 0.2}
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(endpoint, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        out = json.loads(r.read().decode("utf-8", errors="ignore"))

    text = out["candidates"][0]["content"]["parts"][0]["text"]
    # Extract JSON from the model output safely
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return {}
    return json.loads(m.group(0))

def main():
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()

    csv_text = fetch_text(COMMISSION_CSV_URL)
    rows = parse_commission_csv(csv_text)

    items = []
    for r in rows[:200]:  # limit to keep it light
        title = f'{r["changeType"]}: {r["name"]}'.strip(": ").strip()
        base = {
            "title": title,
            "link": "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/compliance-actions/search",
        }

        if api_key:
            ai = gemini_summarize(api_key, title, json.dumps(r))
        else:
            ai = {
              "changeType": "Compliance",
              "affectedService": "All",
              "affectedArea": r.get("state") or "National",
              "impactScore": 40,
              "why": "New/updated compliance action listed by the NDIS Commission.",
              "actionForProviders": "Review the entry and strengthen screening + governance."
            }

        items.append({**base, **ai})

    out = {
      "generated_at": datetime.now(timezone.utc).isoformat(),
      "items": items
    }

    with open("docs/signals.json", "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()

