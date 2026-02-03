// scripts/ingest.js
// STEP 1: Real NDIS enforcement source (CSV) + safe output

const fs = require("fs");
const path = require("path");
const https = require("https");

const SOURCE_URL =
  "https://www.ndiscommission.gov.au/sites/default/files/documents/2024-07/Enforcement-register.csv";
// ↑ If this URL ever changes, only this line is updated.

const OUT_DIR = "public";
const OUT_FILE = path.join(OUT_DIR, "signals.json");

function fetchCsv(url) {
  return new Promise((resolve, reject) => {
    https
      .get(url, (res) => {
        if (res.statusCode !== 200) {
          reject(new Error("Fetch failed: " + res.statusCode));
          return;
        }
        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      })
      .on("error", reject);
  });
}

function parseCsv(csv) {
  const lines = csv.split(/\r?\n/).filter(Boolean);
  const headers = lines.shift().split(",").map((h) => h.trim());

  return lines.map((line) => {
    const cols = line.split(",").map((c) => c.trim());
    const row = {};
    headers.forEach((h, i) => (row[h] = cols[i] || ""));
    return row;
  });
}

function riskFromAction(action) {
  const a = action.toLowerCase();
  if (a.includes("banning") || a.includes("revocation")) return "HIGH";
  if (a.includes("compliance") || a.includes("notice")) return "MED";
  return "LOW";
}

async function run() {
  const csv = await fetchCsv(SOURCE_URL);
  const rows = parseCsv(csv);

  const signals = rows.map((r) => ({
    risk: riskFromAction(r["Action type"] || ""),
    type: r["Action type"] || "",
    name: r["Provider name"] || r["Individual name"] || "",
    state: r["State"] || "",
    effective: r["Effective date"] || "",
    end: r["End date"] || "",
  }));

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(signals, null, 2));

  console.log("NDIS enforcement data ingested:", signals.length);
}

run().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
