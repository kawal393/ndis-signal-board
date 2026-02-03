// scripts/ingest.js
// FINAL: ultra-safe NDIS ingest (never breaks automation)

const fs = require("fs");
const path = require("path");
const https = require("https");
const http = require("http");

const SOURCE_URL =
  "https://www.ndiscommission.gov.au/sites/default/files/documents/2024-07/Enforcement-register.csv";

const OUT_DIR = "public";
const OUT_FILE = path.join(OUT_DIR, "signals.json");

/**
 * Fetch text with full redirect + protocol handling
 */
function fetchText(url, depth = 0) {
  return new Promise((resolve) => {
    if (depth > 5) {
      console.log("Too many redirects, aborting fetch");
      return resolve(null);
    }

    const client = url.startsWith("https") ? https : http;

    client.get(
      url,
      { headers: { "User-Agent": "Mozilla/5.0" } },
      (res) => {
        // Redirect
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
      }
    ).on("error", () => resolve(null));
  });
}

/**
 * VERY forgiving CSV parser
 */
function parseCsv(text) {
  if (!text || !text.includes(",")) return [];

  const lines = text.split(/\r?\n/).filter(Boolean);
  const headers = lines.shift().split(",");

  return lines.map((line) => {
    const cols = line.split(",");
    const row = {};
    headers.forEach((h, i) => (row[h.trim()] = (cols[i] || "").trim()));
    return row;
  });
}

function riskFromAction(action = "") {
  const a = action.toLowerCase();
  if (a.includes("banning") || a.includes("revocation")) return "HIGH";
  if (a.includes("compliance") || a.includes("notice")) return "MED";
  return "LOW";
}

async function run() {
  console.log("Fetching NDIS enforcement data…");

  const text = await fetchText(SOURCE_URL);

  if (!text || text.startsWith("<")) {
    console.log("NDIS source unavailable — keeping last good data");
    return;
  }

  const rows = parseCsv(text);

  if (!rows.length) {
    console.log("No rows parsed — keeping last good data");
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

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(signals, null, 2));

  console.log("Updated signals:", signals.length);
}

run(); // <-- NEVER exit(1)
