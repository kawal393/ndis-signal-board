// scripts/ingest.js
// STEP 2 — Safe, self-healing NDIS ingest with meta status

const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");

// Official NDIS enforcement CSV
const SOURCE_URL =
  "https://www.ndiscommission.gov.au/sites/default/files/documents/2024-07/Enforcement-register.csv";

// Output files used by the website
const OUT_DIR = "public";
const DATA_FILE = path.join(OUT_DIR, "signals.json");
const META_FILE = path.join(OUT_DIR, "meta.json");

/**
 * Fetch text with redirect + protocol handling
 * NEVER throws — always resolves
 */
function fetchText(url, depth = 0) {
  return new Promise((resolve) => {
    if (depth > 5) return resolve(null);

    const client = url.startsWith("https") ? https : http;

    client
      .get(url, { headers: { "User-Agent": "Mozilla/5.0" } }, (res) => {
        // Follow redirects
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return resolve(fetchText(res.headers.location, depth + 1));
        }

        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", () => resolve(null));
  });
}

/**
 * Very forgiving CSV parser
 */
function parseCsv(text) {
  if (!text || !text.includes(",")) return [];

  const lines = text.split(/\r?\n/).filter(Boolean);
  const headers = lines.shift().split(",");

  return lines.map((line) => {
    const cols = line.split(",");
    const row = {};
    headers.forEach((h, i) => {
      row[h.trim()] = (cols[i] || "").trim();
    });
    return row;
  });
}

/**
 * Simple, explainable risk logic
 */
function riskFromAction(action = "") {
  const a = action.toLowerCase();
  if (a.includes("banning") || a.includes("revocation")) return "HIGH";
  if (a.includes("compliance") || a.includes("notice")) return "MED";
  return "LOW";
}

async function run() {
  fs.mkdirSync(OUT_DIR, { recursive: true });

  console.log("Fetching NDIS enforcement data…");
  const text = await fetchText(SOURCE_URL);

  // If source fails, DO NOT touch existing data
  if (!text || text.startsWith("<")) {
    const meta = {
      status: "STALE",
      updated_at: new Date().toISOString(),
      note: "Source unavailable — last good data retained",
    };
    fs.writeFileSync(META_FILE, JSON.stringify(meta, null, 2));
    console.log("Source unavailable — keeping existing data");
    return;
  }

  const rows = parseCsv(text);

  if (!rows.length) {
    const meta = {
      status: "STALE",
      updated_at: new Date().toISOString(),
      note: "No rows parsed — last good data retained",
    };
    fs.writeFileSync(META_FILE, JSON.stringify(meta, null, 2));
    console.log("No rows parsed — keeping existing data");
    return;
  }

  const signals = rows
    .map((r) => ({
      risk: riskFromAction(r["Action type"]),
      type: r["Action type"] || "",
      name: r["Provider name"] || r["Individual name"] || "",
      state: r["State"] || "",
      effective: r["Effective date"] || "",
      end: r["End date"] || "",
    }))
    .filter((r) => r.name && r.type);

  fs.writeFileSync(DATA_FILE, JSON.stringify(signals, null, 2));

  const meta = {
    status: "OK",
    updated_at: new Date().toISOString(),
    records: signals.length,
  };

  fs.writeFileSync(META_FILE, JSON.stringify(meta, null, 2));

  console.log("Updated signals:", signals.length);
}

// Run safely (never crash pipeline)
run();
