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
// PostgreSQL Verbindung
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
// Datenbank initialisieren
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
  `);
  await pool.query(`
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
// Hilfsfunktionen
// --------------------------------------------------

// CSV mit ; oder ,
function parseFlexibleCsv(text) {
  const delimiter = text.includes(";") ? ";" : ",";
  return parse(text, {
    columns: true,
    skip_empty_lines: true,
    trim: true,
    delimiter
  });
}

// Datum normalisieren (YYYY-MM-DD oder DD.MM.YYYY)
function normalizeDate(dateStr) {
  if (!dateStr) return null;

  if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
    return dateStr;
  }

  if (/^\d{2}\.\d{2}\.\d{4}$/.test(dateStr)) {
    const [d, m, y] = dateStr.split(".");
    return `${y}-${m}-${d}`;
  }

  return null;
}

// --------------------------------------------------
// Seiten
// --------------------------------------------------
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// --------------------------------------------------
// Healthcheck
// --------------------------------------------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, message: "DB verbunden" });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "DB nicht erreichbar" });
  }
});
// --------------------------------------------------
// Erlaubte Projekte pro Tag (für Terminal)
// --------------------------------------------------
app.get("/api/allowed-projects", async (req, res) => {
  const { employee_id, date } = req.query;

  if (!employee_id || !date) {
    return res.status(400).json({ error: "employee_id und date erforderlich" });
  }

  try {
    const r = await pool.query(
      `SELECT p.project_id, p.name, epd.approved
       FROM employee_project_day epd
       JOIN projects p ON p.project_id = epd.project_id
       WHERE epd.employee_id = $1 AND epd.work_date = $2`,
      [employee_id, date]
    );

    res.json({ ok: true, projects: r.rows });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Projektabfrage fehlgeschlagen" });
  }
});
// --------------------------------------------------
// Terminal Login (Mitarbeiter-ID)
// --------------------------------------------------
app.get("/api/terminal/login", async (req, res) => {
  const { employee_id } = req.query;
  if (!employee_id) {
    return res.status(400).json({ error: "employee_id fehlt" });
  }

  const r = await pool.query(
    "SELECT employee_id, name, language FROM employees WHERE employee_id = $1",
    [employee_id]
  );

  if (r.rows.length === 0) {
    return res.status(404).json({ error: "Mitarbeiter nicht gefunden" });
  }

  res.json({ ok: true, employee: r.rows[0] });
});

// ==================================================
// CSV IMPORTS
// ==================================================

// --------------------
// Mitarbeiter
// --------------------
app.post("/api/import/employees", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0;
    let skipped = 0;

    for (const r of rows) {
      if (!r.employee_id || !r.name) {
        skipped++;
        continue;
      }

      await pool.query(
        `INSERT INTO employees (employee_id, name, language)
         VALUES ($1,$2,$3)
         ON CONFLICT (employee_id)
         DO UPDATE SET name=$2, language=$3`,
        [r.employee_id.trim(), r.name.trim(), r.language || "de"]
      );
      imported++;
    }

    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Mitarbeiter-Import fehlgeschlagen" });
  }
});

// --------------------
// Projekte
// --------------------
app.post("/api/import/projects", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0;
    let skipped = 0;

    for (const r of rows) {
      if (!r.project_id || !r.name) {
        skipped++;
        continue;
      }

      await pool.query(
        `INSERT INTO projects (project_id, name)
         VALUES ($1,$2)
         ON CONFLICT (project_id)
         DO UPDATE SET name=$2`,
        [r.project_id.trim(), r.name.trim()]
      );
      imported++;
    }

    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Projekt-Import fehlgeschlagen" });
  }
});

// --------------------
// Tageszuordnung (mehrere Tage, robust)
// --------------------
app.post("/api/import/day-projects", async (req, res) => {
  try {
    const rows = parseFlexibleCsv(req.body.csv || "");
    let imported = 0;
    let skipped = 0;

    for (const r of rows) {
      const workDate = normalizeDate(r.work_date);
      if (!r.employee_id || !r.project_id || !workDate) {
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
            workDate,
            r.approved !== "false"
          ]
        );
        imported++;
      } catch (dbErr) {
        console.error("DB Fehler:", r, dbErr.message);
        skipped++;
      }
    }

    res.json({ ok: true, imported, skipped });
  } catch (err) {
    console.error(err);
    res.status(500).json({ ok: false, error: "Tageszuordnung fehlgeschlagen" });
  }
});

// ==================================================
// Server starten
// ==================================================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port " + PORT);
  });
});
