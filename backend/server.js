// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (B5.5 FINAL KOMPLETT)
// ============================================================

const express = require("express");
const path = require("path");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ================= MIDDLEWARE =================
app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ================= ROUTING =================
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);

// ================= DATABASE =================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ================= AUTO MIGRATION =================
async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de',
      daily_hours NUMERIC(4,2) DEFAULT 8.0
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
      total_hours NUMERIC(5,2),
      overtime_hours NUMERIC(5,2) DEFAULT 0
    );
  `);

  console.log("✅ DB migration done");
}

// ================= HELPERS =================
function calcHours(start, end, breaks) {
  return Math.max(
    0,
    (end - start) / 1000 / 60 / 60 - breaks / 60
  );
}

// ================= ROUTES =================
app.get("/api/health", (_, res) => res.json({ ok: true }));

app.get("/api/employees", async (_, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language, daily_hours FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

// ================= ZEITERFASSUNG =================

// START
app.post("/api/time/start", async (req, res) => {
  const { employee_id } = req.body;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,$3)`,
    [employee_id, today, now]
  );

  res.json({ ok: true, message: "Arbeitsbeginn erfasst" });
});

// PAUSE
app.post("/api/time/break", async (req, res) => {
  const { employee_id, minutes } = req.body;
  const today = new Date().toISOString().slice(0, 10);

  await pool.query(
    `UPDATE time_entries
     SET break_minutes = break_minutes + $1
     WHERE employee_id=$2 AND work_date=$3`,
    [minutes, employee_id, today]
  );

  res.json({ ok: true, message: "Pause erfasst" });
});

// ENDE + ÜBERSTUNDEN
app.post("/api/time/end", async (req, res) => {
  const { employee_id, testMinutes } = req.body;

  const realNow = new Date();
  const now = testMinutes
    ? new Date(realNow.getTime() + testMinutes * 60000)
    : realNow;

  const today = now.toISOString().slice(0, 10);

  const entryRes = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY id DESC LIMIT 1`,
    [employee_id, today]
  );

  if (!entryRes.rows.length) {
    return res.status(400).json({ ok: false, error: "Kein Arbeitsbeginn gefunden" });
  }

  const empRes = await pool.query(
    "SELECT daily_hours FROM employees WHERE employee_id=$1",
    [employee_id]
  );

  const entry = entryRes.rows[0];
  const daily = Number(empRes.rows[0].daily_hours || 8);

  const workedHours =
    (now - new Date(entry.start_time)) / 1000 / 60 / 60;

  const autoBreak = workedHours >= 6 ? 30 : 0;

  const total = calcHours(
    new Date(entry.start_time),
    now,
    entry.break_minutes + autoBreak
  );

  const overtime = Math.max(0, total - daily);

  await pool.query(
    `UPDATE time_entries
     SET end_time=$1,
         auto_break_minutes=$2,
         total_hours=$3,
         overtime_hours=$4
     WHERE id=$5`,
    [now, autoBreak, total, overtime, entry.id]
  );

  res.json({
    ok: true,
    totalHours: total.toFixed(2),
    overtimeHours: overtime.toFixed(2)
  });
});

// ================= PDF STUNDENNACHWEIS =================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;
  const activity = req.query.activity || "Montage";

  const empRes = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!empRes.rows.length) return res.sendStatus(404);

  const entries = await pool.query(
    `SELECT work_date, total_hours, overtime_hours
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date`,
    [employeeId]
  );

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // HEADER
  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();
  doc.fontSize(10);
  doc.text(`Mitarbeiter: ${empRes.rows[0].name}`);
  doc.text(`Kalenderwoche: ${kw}`);
  doc.text(`PO: ${po}`);
  doc.text(`Tätigkeit: ${activity}`);
  doc.moveDown(1.5);

  // TABLE HEADER
  doc.font("Helvetica-Bold");
  doc.text("Datum", 40);
  doc.text("Tätigkeit", 140);
  doc.text("Arbeitszeit", 320);
  doc.text("Überstunden", 430);
  doc.moveDown(0.3);
  doc.moveTo(40, doc.y).lineTo(550, doc.y).stroke();
  doc.font("Helvetica");

  let sum = 0;
  let ot = 0;

  entries.rows.forEach(e => {
    const hours = Number(e.total_hours || 0);
    const over = Number(e.overtime_hours || 0);
    sum += hours;
    ot += over;

    doc.text(new Date(e.work_date).toLocaleDateString("de-DE"), 40);
    doc.text(activity, 140);
    doc.text(hours.toFixed(2) + " Std", 320);
    doc.text(over.toFixed(2) + " Std", 430);
    doc.moveDown();
  });

  doc.moveDown(1);
  doc.font("Helvetica-Bold");
  doc.text("Gesamtstunden:", 320);
  doc.text(sum.toFixed(2) + " Std", 430);
  doc.text("Überstunden gesamt:", 320);
  doc.text(ot.toFixed(2) + " Std", 430);
  doc.font("Helvetica");

  doc.moveDown(3);
  doc.text("Ort / Datum: ________________________________");
  doc.moveDown(2);
  doc.text("Unterschrift Mitarbeiter: ________________________________");
  doc.moveDown(2);
  doc.text("Unterschrift Kunde: ________________________________");

  doc.end();
});

// ================= START =================
migrate().then(() => {
  app.listen(PORT, () => {
    console.log("🚀 Server läuft auf Port", PORT);
  });
});
