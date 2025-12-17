// backend/server.js
const express = require("express");
const path = require("path");
const { Pool } = require("pg");
const { parse } = require("csv-parse/sync");

const app = express();
const PORT = process.env.PORT || 10000;

// --------------------------------------------------
// Middleware
// --------------------------------------------------
app.use(express.json({ limit: "5mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// --------------------------------------------------
// PostgreSQL
// --------------------------------------------------
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// --------------------------------------------------
// DB Init
// --------------------------------------------------
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

    CREATE TABLE IF NOT EXISTS time_events (
      id SERIAL PRIMARY KEY,
      employee_id TEXT REFERENCES employees(employee_id),
      project_id TEXT REFERENCES projects(project_id),
      event_type TEXT NOT NULL,
      event_time TIMESTAMP NOT NULL DEFAULT NOW(),
      approved BOOLEAN NOT NULL DEFAULT true
    );
  `);
}

// --------------------------------------------------
// Helpers
// --------------------------------------------------
function parseFlexibleCsv(text) {
  const delimiter = text.includes(";") ? ";" : ",";
  return parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    delimiter
  });
}

function normalizeDate(dateStr) {
  if (!dateStr) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) return dateStr;
  if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
    const [d, m, y] = dateStr.split(".");
    return `${y}-${m}-${d}`;
  }
  return null;
}

// --------------------------------------------------
// Pages
// --------------------------------------------------
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// --------------------------------------------------
// Health
// --------------------------------------------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch {
    res.status(500).json({ ok: false });
  }
});

// --------------------------------------------------
// CSV IMPORTS
// --------------------------------------------------
app.post("/api/import/employees", async (req, res) => {
  const rows = parseFlexibleCsv(req.body.csv || "");
  let imported = 0;

  for (const r of rows) {
    if (!r.employee_id || !r.name) continue;
    await pool.query(
      `INSERT INTO employees (employee_id,name,language)
       VALUES ($1,$2,$3)
       ON CONFLICT (employee_id)
       DO UPDATE SET name=$2, language=$3`,
      [r.employee_id, r.name, r.language || "de"]
    );
    imported++;
  }
  res.json({ ok: true, imported });
});

app.post("/api/import/projects", async (req, res) => {
  const rows = parseFlexibleCsv(req.body.csv || "");
  let imported = 0;

  for (const r of rows) {
    if (!r.project_id || !r.name) continue;
    await pool.query(
      `INSERT INTO projects (project_id,name)
       VALUES ($1,$2)
       ON CONFLICT (project_id)
       DO UPDATE SET name=$2`,
      [r.project_id, r.name]
    );
    imported++;
  }
  res.json({ ok: true, imported });
});

app.post("/api/import/day-projects", async (req, res) => {
  const rows = parseFlexibleCsv(req.body.csv || "");
  let imported = 0;

  for (const r of rows) {
    const d = normalizeDate(r.work_date);
    if (!r.employee_id || !r.project_id || !d) continue;

    await pool.query(
      `INSERT INTO employee_project_day
       (employee_id,project_id,work_date,approved)
       VALUES ($1,$2,$3,$4)
       ON CONFLICT (employee_id,project_id,work_date)
       DO UPDATE SET approved=$4`,
      [r.employee_id, r.project_id, d, r.approved !== "false"]
    );
    imported++;
  }
  res.json({ ok: true, imported });
});

// --------------------------------------------------
// TERMINAL APIs
// --------------------------------------------------
app.get("/api/terminal/login", async (req, res) => {
  const { employee_id } = req.query;
  const r = await pool.query(
    "SELECT employee_id,name,language FROM employees WHERE employee_id=$1",
    [employee_id]
  );
  if (!r.rows.length) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});

app.get("/api/allowed-projects", async (req, res) => {
  const { employee_id, date } = req.query;
  const r = await pool.query(
    `SELECT p.project_id,p.name
     FROM employee_project_day ep
     JOIN projects p ON p.project_id=ep.project_id
     WHERE ep.employee_id=$1 AND ep.work_date=$2`,
    [employee_id, date]
  );
  res.json({ ok: true, projects: r.rows });
});

app.post("/api/time-event", async (req, res) => {
  const { employee_id, project_id, event_type } = req.body;
  await pool.query(
    `INSERT INTO time_events (employee_id,project_id,event_type)
     VALUES ($1,$2,$3)`,
    [employee_id, project_id, event_type]
  );
  res.json({ ok: true });
});

// --------------------------------------------------
initDb().then(() => {
  app.listen(PORT, () =>
    console.log("Server läuft auf Port " + PORT)
  );
});
