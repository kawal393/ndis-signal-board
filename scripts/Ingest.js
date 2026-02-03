// scripts/Ingest.js
// Minimal working ingest script (CommonJS – guaranteed to run)

const fs = require("fs");
const path = require("path");

// Where the website reads data from
const OUT_DIR = "public";
const OUT_FILE = path.join(OUT_DIR, "signals.json");

// Dummy data (temporary – proves automation)
const signals = [
  {
    risk: "HIGH",
    type: "ER - Banning Order",
    name: "Example Provider Pty Ltd",
    state: "VIC",
    effective: "2026-02-03",
    end: "",
  },
  {
    risk: "MED",
    type: "ER - Compliance Notice",
    name: "Second Example Care Pty Ltd",
    state: "NSW",
    effective: "2026-02-01",
    end: "",
  }
];

// Ensure output directory exists
fs.mkdirSync(OUT_DIR, { recursive: true });

// Write signals.json
fs.writeFileSync(OUT_FILE, JSON.stringify(signals, null, 2));

console.log("signals.json written successfully");
