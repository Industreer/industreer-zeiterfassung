// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (B5 IST-ZEITEN)
// ============================================================

const express = require("express");
const path = require("path");
const crypto = require("crypto");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ================= MIDDLEWARE =================
app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ================= DATABASE =================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ================= INIT DB =================
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      work_date DATE,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      break_minutes INT DEFAULT 0,
      auto_break_minutes INT DEFAULT 0,
      total_hours NUMERIC(5,2)
    );
  `);
}

// ================= HELPERS =================
function calculateHours(start, end, breaks, autoBreak) {
  const diffMs = end - start;
  const diffHours = diffMs / 1000 / 60 / 60;
  const totalBreak = (breaks + autoBreak) / 60;
  return Math.max(0, diffHours - totalBreak);
}

// ================= ROUTES =================

// ---- HEALTH ----
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ---- EMPLOYEES ----
app.get("/api/employees", async (_, res) => {
  const r = await pool.query("SELECT * FROM employees ORDER BY name");
  res.json(r.rows);
});

// ================= ZEITERFASSUNG =================

// Arbeitsbeginn
app.post("/api/time/start", async (req, res) => {
  const { employee_id } = req.body;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  await pool.query(
    `
    INSERT INTO time_entries (employee_id, work_date, start_time)
    VALUES ($1,$2,$3)
    `,
    [employee_id, today, now]
  );

  res.json({ ok: true, message: "Arbeitsbeginn erfasst" });
});

// Raucherpause
app.post("/api/time/break", async (req, res) => {
  const { employee_id, minutes } = req.body;
  const today = new Date().toISOString().slice(0, 10);

  await pool.query(
    `
    UPDATE time_entries
    SET break_minutes = break_minutes + $1
    WHERE employee_id=$2 AND work_date=$3
    `,
    [minutes, employee_id, today]
  );

  res.json({ ok: true, message: "Pause erfasst" });
});

// Arbeitsende
app.post("/api/time/end", async (req, res) => {
  const { employee_id } = req.body;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  const r = await pool.query(
    `
    SELECT * FROM time_entries
    WHERE employee_id=$1 AND work_date=$2
    `,
    [employee_id, today]
  );

  if (!r.rows.length) {
    return res.status(400).json({ ok: false, error: "Kein Start gefunden" });
  }

  const entry = r.rows[0];
  const start = new Date(entry.start_time);

  // gesetzliche Pause: 30 Min ab 6 Std
  const workedHours = (now - start) / 1000 / 60 / 60;
  const autoBreak = workedHours >= 6 ? 30 : 0;

  const totalHours = calculateHours(
    start,
    now,
    entry.break_minutes,
    autoBreak
  );

  await pool.query(
    `
    UPDATE time_entries
    SET end_time=$1,
        auto_break_minutes=$2,
        total_hours=$3
    WHERE id=$4
    `,
    [now, autoBreak, totalHours, entry.id]
  );

  res.json({ ok: true, totalHours });
});

// ================= PDF (JETZT MIT IST-ZEITEN) =================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.status(404).send("Mitarbeiter nicht gefunden");

  const employeeName = emp.rows[0].name;

  const entries = await pool.query(
    `
    SELECT work_date, total_hours
    FROM time_entries
    WHERE employee_id=$1
    ORDER BY work_date
    `,
    [employeeId]
  );

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();
  doc.text(`Name: ${employeeName}`);
  doc.text(`Kalenderwoche: ${kw}`);
  doc.text(`PO: ${po}`);
  doc.moveDown();

  let sum = 0;
  entries.rows.forEach(e => {
    sum += Number(e.total_hours || 0);
    doc.text(
      `${new Date(e.work_date).toLocaleDateString("de-DE")}  ${Number(
        e.total_hours
      ).toFixed(2)} Std`
    );
  });

  doc.moveDown();
  doc.text(`Gesamtstunden: ${sum.toFixed(2)}`);

  doc.end();
});

// ================= START =================
initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
