// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (B5.5 ÜBERSTUNDEN)
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

// ================= INIT DB =================
async function initDb() {
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
      overtime_hours NUMERIC(5,2)
    );
  `);
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
    "SELECT employee_id, name, email, language, daily_hours FROM employees"
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

  res.json({ ok: true });
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

  res.json({ ok: true });
});

// ENDE + ÜBERSTUNDEN
app.post("/api/time/end", async (req, res) => {
  const { employee_id } = req.body;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  const entryRes = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY id DESC LIMIT 1`,
    [employee_id, today]
  );
  if (!entryRes.rows.length)
    return res.status(400).json({ error: "Kein Start gefunden" });

  const empRes = await pool.query(
    "SELECT daily_hours FROM employees WHERE employee_id=$1",
    [employee_id]
  );

  const entry = entryRes.rows[0];
  const daily = Number(empRes.rows[0].daily_hours || 8);
  const autoBreak =
    (now - new Date(entry.start_time)) / 1000 / 60 / 60 >= 6 ? 30 : 0;

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

// ================= PDF =================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

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

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();

  let sum = 0, ot = 0;

  entries.rows.forEach(e => {
    sum += Number(e.total_hours);
    ot += Number(e.overtime_hours || 0);
    doc.text(
      `${new Date(e.work_date).toLocaleDateString("de-DE")}  ${Number(
        e.total_hours
      ).toFixed(2)} Std  (+${Number(e.overtime_hours).toFixed(2)} ÜStd)`
    );
  });

  doc.moveDown();
  doc.text(`Gesamtstunden: ${sum.toFixed(2)}`);
  doc.text(`Überstunden: ${ot.toFixed(2)}`);
  doc.end();
});

// ================= START =================
initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
