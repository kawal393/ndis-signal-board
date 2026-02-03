import csv
import io
import json
import re
from datetime import datetime, timezone
from urllib.request import urlopen, Request

COMMISSION_CSV_URL = "https://www.ndiscommission.gov.au/about-us/compliance-and-enforcement/compliance-actions/search/export"
OUTPUT_PATH = "docs/signals.json"


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", errors="ignore")


def norm_key(s: str) -> str:
    # normalize header keys: "Date effective from" -> "date_effective_from"
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s).strip("_")
    return s


def pick(d: dict, *keys: str) -> str:
    # pick first non-empty match from possible keys
    for k in keys:
        v = d.get(k, "")
        if v is None:
            continue
        v = str(v).strip()
        if v:
            return v
    return ""


def to_iso_date(s: str) -> str:
    # Keep it simple: if it's already ISO-ish, return; if it's "YYYY-MM-DD ..." return date part.
    s = (s or "").strip()
    if not s:
        return ""
    # Common export looks like "2025-08-13 17:00" or similar
    m = re.match(r"^\d{4}-\d{2}-\d{2}", s)
    if m:
        return m.group(0)
    return s  # fallback


def risk_from_type(t: str) -> str:
    t_low = (t or "").lower()
    if "banning" in t_low:
        return "HIGH"
    if "revocation" in t_low:
        return "HIGH"
    if "enforceable" in t_low:
        return "MED"
    if "compliance" in t_low:
        return "MED"
    if "infringement" in t_low:
        return "MED"
    return "LOW"


def parse_csv(csv_text: str) -> list[dict]:
    # Some CSVs have BOM; csv module handles it better if we strip it
    csv_text = csv_text.lstrip("\ufeff")

    reader = csv.DictReader(io.StringIO(csv_text))
    # Normalize headers once per row by rebuilding dict with normalized keys
    signals: list[dict] = []

    for row in reader:
        row_norm = {norm_key(k): (v or "").strip() for k, v in row.items()}

        action_type = pick(
            row_norm,
            "type",
            "action_type",
            "compliance_action_type",
        )

        effective_raw = pick(
            row_norm,
            "date_effective_from",
            "date_effective",
            "effective_from",
            "date_effective_from_aedt",
        )

        end_raw = pick(
            row_norm,
            "date_no_longer_in_effect",
            "no_longer_in_effect",
            "end_date",
            "date_end",
        )

        name = pick(row_norm, "name", "provider_name", "entity_name")
        state = pick(row_norm, "state", "jurisdiction")

        # Skip empty junk rows
        if not (action_type or name or effective_raw):
            continue

        signals.append(
            {
                "risk": risk_from_type(action_type),
                "type": action_type,
                "name": name,
                "state": state,
                "effective": to_iso_date(effective_raw),
                "end": to_iso_date(end_raw),
            }
        )

    return signals


def main():
    try:
        csv_text = fetch_text(COMMISSION_CSV_URL)
    except Exception as e:
        print("Fetch failed:", e)
        return

    signals = parse_csv(csv_text)

    data = {
        "updated_utc": datetime.now(timezone.utc).isoformat(),
        "count": len(signals),
        "signals": signals,
        # Keep a tiny preview for debugging
        "raw_preview": csv_text[:300],
    }

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"signals.json updated: {len(signals)} signals")


if __name__ == "__main__":
    main()
