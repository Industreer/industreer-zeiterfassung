// backend/server.js
const express = require("express");
const path = require("path");
const { Pool } = require("pg");
const { parse } = require("csv-parse/sync");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "10mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// --------------------
// DB
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

    CREATE TABLE IF NOT EXISTS time_events (
      id SERIAL PRIMARY KEY,
      employee_id TEXT REFERENCES employees(employee_id),
      project_id TEXT NOT NULL,
      event_type TEXT NOT NULL,              -- IN | PAUSE | OUT
      event_time TIMESTAMP NOT NULL DEFAULT NOW(),
      approved BOOLEAN NOT NULL DEFAULT true,
      is_exception BOOLEAN NOT NULL DEFAULT false,
      note TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_time_events_emp_time
      ON time_events(employee_id, event_time DESC);
  `);
}

// --------------------
// Helpers
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

function normalizeDate(dateStr) {
  if (!dateStr) return null;

  const s = String(dateStr).trim();

  // ISO: 2025-12-17
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) return s;

  // DE: 17.12.2025
  if (/^\d{2}\.\d{2}\.\d{4}$/.test(s)) {
    const [d, m, y] = s.split(".");
    return `${y}-${m}-${d}`;
  }

  return null;
}

async function isProjectAllowedToday(employee_id, project_id, work_date_iso) {
  const r = await pool.query(
    `SELECT approved
     FROM employee_project_day
     WHERE employee_id=$1 AND project_id=$2 AND work_date=$3`,
    [employee_id, project_id, work_date_iso]
  );
  if (!r.rows.length) return { allowed: false, approved: false };
  return { allowed: true, approved: !!r.rows[0].approved };
}

function isoToday() {
  return new Date().toISOString().slice(0, 10);
}

// --------------------
// Pages
// --------------------
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// --------------------
// Health
// --------------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, message: "DB verbunden" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "DB nicht erreichbar" });
  }
});

// --------------------
// Terminal Login
// --------------------
app.get("/api/terminal/login", async (req, res) => {
  const { employee_id } = req.query;
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const r = await pool.query(
    "SELECT employee_id, name, language FROM employees WHERE employee_id=$1",
    [String(employee_id).trim()]
  );

  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

  res.json({ ok: true, employee: r.rows[0] });
});

// --------------------
// Allowed projects for day
// --------------------
app.get("/api/allowed-projects", async (req, res) => {
  const { employee_id, date } = req.query;
  if (!employee_id || !date) {
    return res.status(400).json({ ok: false, error: "employee_id und date erforderlich" });
  }

  const d = normalizeDate(date) || String(date).trim(); // date kommt vom Terminal als ISO, aber wir lassen beides zu

  try {
    const r = await pool.query(
      `SELECT p.project_id, p.name, epd.approved
       FROM employee_project_day epd
       JOIN projects p ON p.project_id = epd.project_id
       WHERE epd.employee_id = $1 AND epd.work_date = $2
       ORDER BY p.project_id`,
      [String(employee_id).trim(), d]
    );

    res.json({ ok: true, projects: r.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Projektabfrage fehlgeschlagen" });
  }
});

// ==================================================
// CSV IMPORTS
// ==================================================
app.post("/api/import/employees", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0, skipped = 0;

    for (const r of rows) {
      if (!r.employee_id || !r.name) { skipped++; continue; }
      await pool.query(
        `INSERT INTO employees (employee_id, name, language)
         VALUES ($1,$2,$3)
         ON CONFLICT (employee_id)
         DO UPDATE SET name=$2, language=$3`,
        [String(r.employee_id).trim(), String(r.name).trim(), (r.language || "de").trim()]
      );
      imported++;
    }
    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Mitarbeiter-Import fehlgeschlagen" });
  }
});

app.post("/api/import/projects", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0, skipped = 0;

    for (const r of rows) {
      if (!r.project_id || !r.name) { skipped++; continue; }
      await pool.query(
        `INSERT INTO projects (project_id, name)
         VALUES ($1,$2)
         ON CONFLICT (project_id)
         DO UPDATE SET name=$2`,
        [String(r.project_id).trim(), String(r.name).trim()]
      );
      imported++;
    }
    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Projekt-Import fehlgeschlagen" });
  }
});

app.post("/api/import/day-projects", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0, skipped = 0, errors = 0;

    for (const r of rows) {
      const employee_id = r.employee_id ? String(r.employee_id).trim() : "";
      const project_id = r.project_id ? String(r.project_id).trim() : "";
      const work_date = normalizeDate(r.work_date);

      if (!employee_id || !project_id || !work_date) { skipped++; continue; }

      const approved = String(r.approved ?? "true").trim().toLowerCase() !== "false";

      try {
        await pool.query(
          `INSERT INTO employee_project_day (employee_id, project_id, work_date, approved)
           VALUES ($1,$2,$3,$4)
           ON CONFLICT (employee_id, project_id, work_date)
           DO UPDATE SET approved=$4`,
          [employee_id, project_id, work_date, approved]
        );
        imported++;
      } catch (dbErr) {
        errors++;
        console.error("DB Fehler day-project:", r, dbErr.message);
      }
    }

    res.json({ ok: true, imported, skipped, errors });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Tageszuordnung-Import fehlgeschlagen" });
  }
});

// ==================================================
// TIME EVENT (IN/PAUSE/OUT) + Exception Handling
// ==================================================
app.post("/api/time-event", async (req, res) => {
  try {
    const { employee_id, project_id, event_type, date, note } = req.body;

    if (!employee_id || !project_id || !event_type) {
      return res.status(400).json({ ok: false, error: "employee_id, project_id, event_type erforderlich" });
    }

    const emp = String(employee_id).trim();
    const proj = String(project_id).trim();
    const type = String(event_type).trim().toUpperCase();

    if (!["IN", "PAUSE", "OUT"].includes(type)) {
      return res.status(400).json({ ok: false, error: "event_type muss IN, PAUSE oder OUT sein" });
    }

    const workDate = normalizeDate(date) || isoToday();

    // Prüfen, ob Projekt regulär erlaubt ist
    const allow = await isProjectAllowedToday(emp, proj, workDate);

    const is_exception = !allow.allowed;
    const approved = allow.allowed ? allow.approved : false;

    await pool.query(
      `INSERT INTO time_events (employee_id, project_id, event_type, approved, is_exception, note)
       VALUES ($1,$2,$3,$4,$5,$6)`,
      [emp, proj, type, approved, is_exception, note ? String(note).trim() : null]
    );

    res.json({ ok: true, approved, is_exception });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Zeitbuchung fehlgeschlagen" });
  }
});

// --------------------
// Start
// --------------------
initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port " + PORT));
});
