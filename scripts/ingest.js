// scripts/ingest.js
// STEP 1 — Real NDIS enforcement data ingest (redirect-safe, minimal)

const fs = require("fs");
const path = require("path");
const https = require("https");

// Official NDIS enforcement register (CSV)
const SOURCE_URL =
  "https://www.ndiscommission.gov.au/sites/default/files/documents/2024-07/Enforcement-register.csv";

// Where your site reads data from
const OUT_DIR = "public";
const OUT_FILE = path.join(OUT_DIR, "signals.json");

/**
 * Fetch CSV and FOLLOW redirects (NDIS uses redirects)
 */
function fetchCsv(url) {
  return new Promise((resolve, reject) => {
    https.get(
      url,
      { headers: { "User-Agent": "apex-watchtower" } },
      (res) => {
        // Follow redirects
        if (
          res.statusCode >= 300 &&
          res.statusCode < 400 &&
          res.headers.location
        ) {
          return resolve(fetchCsv(res.headers.location));
        }

        if (res.statusCode !== 200) {
          reject(new Error("Fetch failed: " + res.statusCode));
          return;
        }

        let data = "";
        res.on("data", (chunk) => (data += chunk));
        res.on("end", () => resolve(data));
      }
    ).on("error", reject);
  });
}

/**
 * Very simple CSV parser (sufficient for NDIS register)
 */
function parseCsv(csv) {
  const lines = csv.split(/\r?\n/).filter(Boolean);
  const headers = lines.shift().split(",").map((h) => h.trim());

  return lines.map((line) => {
    const cols = line.split(",").map((c) => c.trim());
    const row = {};
    headers.forEach((h, i) => {
      row[h] = cols[i] || "";
    });
    return row;
  });
}

/**
 * Risk classification (minimal + deterministic)
 */
function riskFromAction(action) {
  const a = action.toLowerCase();
  if (a.includes("banning") || a.includes("revocation")) return "HIGH";
  if (a.includes("compliance") || a.includes("notice")) return "MED";
  return "LOW";
}

async function run() {
  const csv = await fetchCsv(SOURCE_URL);
  const rows = parseCsv(csv);

  const signals = rows
    .map((r) => ({
      risk: riskFromAction(r["Action type"] || ""),
      type: r["Action type"] || "",
      name: r["Provider name"] || r["Individual name"] || "",
      state: r["State"] || "",
      effective: r["Effective date"] || "",
      end: r["End date"] || "",
    }))
    .filter((r) => r.name && r.type); // basic sanity filter

  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.writeFileSync(OUT_FILE, JSON.stringify(signals, null, 2));

  console.log("NDIS enforcement records ingested:", signals.length);
}

// Execute
run().catch((err) => {
  console.error(err.message);
  process.exit(1);
});
