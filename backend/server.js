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

    CREATE TABLE IF NOT EXISTS employee_project_day (
      employee_id TEXT REFERENCES employees(employee_id),
      project_id TEXT REFERENCES projects(project_id),
      work_date DATE NOT NULL,
      approved BOOLEAN NOT NULL DEFAULT true,
      PRIMARY KEY (employee_id, project_id, work_date)
    );
  `);
}

// --------------------
// CSV Helper (; oder ,)
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

// ======================================================
// CSV IMPORTS (JETZT WIEDER VOLLSTÄNDIG)
// ======================================================

// Mitarbeiter
app.post("/api/import/employees", async (req, res) => {
  const rows = parseFlexibleCsv(req.body.csv || "");
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

// Projekte
app.post("/api/import/projects", async (req, res) => {
  const rows = parseFlexibleCsv(req.body.csv || "");
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

// Tageszuordnung (robust, kein Server-Crash)
app.post("/api/import/day-projects", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0;
    let skipped = 0;

    for (const r of rows) {
      if (!r.employee_id || !r.project_id || !r.work_date) {
        skipped++;
        continue;
      }

      try {
        await pool.query(
          `INSERT INTO employee_project_day
           (employee_id, project_id, work_date, approved)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (employee_id, project_id, work_date)
           DO UPDATE SET approved=$4`,
          [
            r.employee_id.trim(),
            r.project_id.trim(),
            r.work_date.trim(),
            r.approved !== "false"
          ]
        );
        imported++;
      } catch (dbErr) {
        console.error("DB Fehler bei Zeile:", r, dbErr.message);
        skipped++;
      }
    }

    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error("CSV Import Fehler:", err);
    res.status(500).json({ ok: false, error: "CSV Import fehlgeschlagen" });
  }
});

// --------------------
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port " + PORT);
  });
});
