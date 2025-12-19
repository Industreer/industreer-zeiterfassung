// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (PDF LOGO FINAL FIX)
// ============================================================

const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ============================================================
// MIDDLEWARE
// ============================================================
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ============================================================
// PAGE ROUTES
// ============================================================
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);

// ============================================================
// DATABASE
// ============================================================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ============================================================
// AUTO MIGRATION
// ============================================================
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
      total_hours NUMERIC(6,2),
      overtime_hours NUMERIC(6,2) DEFAULT 0
    );
  `);

  console.log("✅ DB migration done");
}

// ============================================================
// LOGO STORAGE (PNG, PDF-KOMPATIBEL)
// ============================================================
const LOGO_PATH = path.join(__dirname, "logo.png");

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    if (!file.mimetype.startsWith("image/")) {
      return cb(new Error("Only image files allowed"));
    }
    cb(null, true);
  }
});

// Upload logo
app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  try {
    fs.writeFileSync(LOGO_PATH, req.file.buffer);
    res.json({ ok: true });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message });
  }
});

// Serve logo
app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_PATH)) return res.sendStatus(404);
  res.sendFile(LOGO_PATH);
});

// ============================================================
// HEALTH
// ============================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ============================================================
// TIME TRACKING
// ============================================================
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

app.post("/api/time/end", async (req, res) => {
  const { employee_id } = req.body;
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY id DESC LIMIT 1`,
    [employee_id, today]
  );

  if (!r.rows.length) return res.status(400).json({ ok: false });

  const entry = r.rows[0];
  const worked = (now - new Date(entry.start_time)) / 36e5;
  const autoBreak = worked >= 6 ? 30 : 0;
  const total = Math.max(0, worked - (entry.break_minutes + autoBreak) / 60);

  const emp = await pool.query(
    "SELECT daily_hours FROM employees WHERE employee_id=$1",
    [employee_id]
  );

  const daily = Number(emp.rows[0]?.daily_hours || 8);
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

  res.json({ ok: true, totalHours: total.toFixed(2) });
});

// ============================================================
// PDF TIMESHEET (MIT LOGO)
// ============================================================
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

  // LOGO
  if (fs.existsSync(LOGO_PATH)) {
    doc.image(LOGO_PATH, 40, 30, { width: 120 });
    doc.moveDown(3);
  }

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();

  doc.fontSize(10);
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`);
  doc.text(`KW: ${kw}`);
  doc.text(`PO: ${po}`);
  doc.moveDown();

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40);
  doc.text("Arbeitszeit", 200);
  doc.text("Überstunden", 330);
  doc.moveDown(0.5);
  doc.font("Helvetica");

  entries.rows.forEach(e => {
    doc.text(new Date(e.work_date).toLocaleDateString("de-DE"), 40);
    doc.text((e.total_hours || 0) + " Std", 200);
    doc.text((e.overtime_hours || 0) + " Std", 330);
    doc.moveDown();
  });

  doc.end();
});

// ============================================================
// START
// ============================================================
migrate().then(() => {
  app.listen(PORT, () => {
    console.log("🚀 Server läuft auf Port", PORT);
  });
});
