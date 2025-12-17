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
// Hilfsfunktion CSV (; oder ,)
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
// API: erlaubte Projekte pro Tag (für Terminal)
// --------------------
app.get("/api/allowed-projects", async (req, res) => {
  const { employee_id, date } = req.query;
  if (!employee_id || !date) {
    return res.status(400).json({ error: "employee_id und date erforderlich" });
  }

  const r = await pool.query(
    `SELECT p.project_id, p.name, epd.approved
     FROM employee_project_day epd
     JOIN projects p ON p.project_id = epd.project_id
     WHERE epd.employee_id = $1 AND epd.work_date = $2`,
    [employee_id, date]
  );

  res.json({ projects: r.rows });
});

// --------------------
// CSV IMPORT – Tageszuordnung
// --------------------
app.post("/api/import/day-projects", async (req, res) => {
  if (!req.body.csv) {
    return res.status(400).json({ error: "CSV fehlt" });
  }

  const rows = parseFlexibleCsv(req.body.csv);
  let imported = 0;

  for (const r of rows) {
    if (!r.employee_id || !r.project_id || !r.work_date) continue;

    await pool.query(
      `INSERT INTO employee_project_day
       (employee_id, project_id, work_date, approved)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (employee_id, project_id, work_date)
       DO UPDATE SET approved=$4`,
      [
        r.employee_id,
        r.project_id,
        r.work_date,
        r.approved !== "false"
      ]
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
