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
app.use(express.json({ limit: "10mb" }));
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
      project_id TEXT NOT NULL,
      event_type TEXT NOT NULL,
      event_time TIMESTAMP NOT NULL DEFAULT NOW(),
      approved BOOLEAN NOT NULL DEFAULT true,
      is_exception BOOLEAN NOT NULL DEFAULT false,
      note TEXT
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
  const s = String(dateStr).trim();

  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;
  if (/^\d{2}\.\d{2}\.\d{4}$/.test(s)) {
    const [d, m, y] = s.split(".");
    return `${y}-${m}-${d}`;
  }
  return null;
}

function todayIso() {
  return new Date().toISOString().slice(0, 10);
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
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false });
  }
});

// --------------------------------------------------
// Terminal Login
// --------------------------------------------------
app.get("/api/terminal/login", async (req, res) => {
  const { employee_id } = req.query;
  if (!employee_id) {
    return res.status(400).json({ ok: false, error: "employee_id fehlt" });
  }

  const r = await pool.query(
    "SELECT employee_id, name, language FROM employees WHERE employee_id=$1",
    [String(employee_id).trim()]
  );

  if (!r.rows.length) {
    return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });
  }

  res.json({ ok: true, employee: r.rows[0] });
});

// --------------------------------------------------
// Erlaubte Projekte für Tag
// --------------------------------------------------
app.get("/api/allowed-projects", async (req, res) => {
  const { employee_id, date } = req.query;
  if (!employee_id || !date) {
    return res.status(400).json({ ok: false, error: "employee_id oder date fehlt" });
  }

  const d = normalizeDate(date) || date;

  const r = await pool.query(
    `SELECT p.project_id, p.name
     FROM employee_project_day ep
     JOIN projects p ON p.project_id = ep.project_id
     WHERE ep.employee_id = $1 AND ep.work_date = $2`,
    [String(employee_id).trim(), d]
  );

  res.json({ ok: true, projects: r.rows });
});

// ==================================================
// CSV IMPORTS
// ==================================================
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

// ==================================================
// ZEITERFASSUNG (FINAL & ROBUST)
// ==================================================
app.post("/api/time-event", async (req, res) => {
  try {
    let { employee_id, project_id, event_type, date, note } = req.body;

    if (!employee_id || !event_type) {
      return res.status(400).json({ ok: false, error: "employee_id oder event_type fehlt" });
    }

    employee_id = String(employee_id).trim();
    event_type = String(event_type).trim().toUpperCase();
    project_id = project_id ? String(project_id).trim() : "";

    if (!project_id) {
      return res.status(400).json({ ok: false, error: "project_id fehlt" });
    }

    if (!["IN", "PAUSE", "OUT"].includes(event_type)) {
      return res.status(400).json({ ok: false, error: "Ungültiger event_type" });
    }

    const workDate = normalizeDate(date) || todayIso();

    const r = await pool.query(
      `SELECT approved
       FROM employee_project_day
       WHERE employee_id=$1 AND project_id=$2 AND work_date=$3`,
      [employee_id, project_id, workDate]
    );

    const is_exception = r.rows.length === 0;
    const approved = is_exception ? false : !!r.rows[0].approved;

    await pool.query(
      `INSERT INTO time_events
       (employee_id, project_id, event_type, approved, is_exception, note)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [employee_id, project_id, event_type, approved, is_exception, note || null]
    );

    res.json({ ok: true, approved, is_exception });
  } catch (err) {
    console.error("TIME EVENT ERROR:", err);
    res.status(500).json({ ok: false, error: "Zeitbuchung fehlgeschlagen" });
  }
});

// --------------------------------------------------
initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port " + PORT));
});
