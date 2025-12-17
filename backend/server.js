// backend/server.js
const express = require("express");
const path = require("path");
const { Pool } = require("pg");
const { parse } = require("csv-parse/sync");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// --------------------
// PostgreSQL
// --------------------
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      language TEXT NOT NULL DEFAULT 'de'
    );
    CREATE TABLE IF NOT EXISTS projects (
      project_id TEXT PRIMARY KEY,
      name TEXT NOT NULL
    );
  `);
}

// --------------------
// Hilfsfunktion: CSV flexibel lesen
// --------------------
function parseFlexibleCsv(text) {
  const delimiter = text.includes(";") ? ";" : ",";
  return parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    delimiter
  });
}

// --------------------
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// --------------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, message: "DB verbunden" });
  } catch {
    res.status(500).json({ ok: false, error: "DB nicht erreichbar" });
  }
});

// --------------------
// CSV IMPORT – Mitarbeiter
// --------------------
app.post("/api/import/employees", async (req, res) => {
  if (!req.body.csv) {
    return res.status(400).json({ error: "CSV fehlt" });
  }

  const rows = parseFlexibleCsv(req.body.csv);
  let imported = 0;

  for (const r of rows) {
    if (!r.employee_id || !r.name) continue;

    await pool.query(
      `INSERT INTO employees (employee_id, name, language)
       VALUES ($1,$2,$3)
       ON CONFLICT (employee_id)
       DO UPDATE SET name=$2, language=$3`,
      [r.employee_id, r.name, r.language || "de"]
    );
    imported++;
  }

  res.json({ ok: true, imported });
});

// --------------------
// CSV IMPORT – Projekte
// --------------------
app.post("/api/import/projects", async (req, res) => {
  if (!req.body.csv) {
    return res.status(400).json({ error: "CSV fehlt" });
  }

  const rows = parseFlexibleCsv(req.body.csv);
  let imported = 0;

  for (const r of rows) {
    if (!r.project_id || !r.name) continue;

    await pool.query(
      `INSERT INTO projects (project_id, name)
       VALUES ($1,$2)
       ON CONFLICT (project_id)
       DO UPDATE SET name=$2`,
      [r.project_id, r.name]
    );
    imported++;
  }

  res.json({ ok: true, imported });
});

// --------------------
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port " + PORT);
  });
});
