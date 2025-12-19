// ======================================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL)
// - Mitarbeitername + Timer
// - Staffplan Import (robust)
// - Zeitbuchung
// - PDF mit Start- & Endzeiten
// ======================================================================

const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ======================================================================
// MIDDLEWARE
// ======================================================================
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ======================================================================
// PAGES
// ======================================================================
app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

// ======================================================================
// DATABASE
// ======================================================================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ======================================================================
// MIGRATION
// ======================================================================
async function migrate() {
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
      total_hours NUMERIC(6,2),
      activity TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT,
      employee_name TEXT,
      work_date DATE,
      customer_name TEXT,
      customer_po TEXT,
      internal_po TEXT
    );
  `);
}

// ======================================================================
// UPLOAD
// ======================================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================================
// LOGO
// ======================================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE) || !fs.existsSync(LOGO_META)) return res.sendStatus(404);
  const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ======================================================================
// EMPLOYEE LOOKUP
// ======================================================================
app.get("/api/employee/:id", async (req, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1",
    [req.params.id]
  );
  if (!r.rows.length) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});

// ======================================================================
// ZEITBUCHUNG
// ======================================================================
app.post("/api/time/start", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  const r = await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())
     RETURNING start_time`,
    [req.body.employee_id, today]
  );
  res.json({ ok: true, start_time: r.rows[0].start_time });
});

app.post("/api/time/end", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY start_time DESC LIMIT 1`,
    [req.body.employee_id, today]
  );
  if (!r.rows.length) return res.status(400).json({ ok: false });

  const start = new Date(r.rows[0].start_time);
  const end = new Date();
  const hours = (end - start) / 3600000;

  await pool.query(
    `UPDATE time_entries
     SET end_time=NOW(), total_hours=$1, activity=$2
     WHERE id=$3`,
    [hours, req.body.activity, r.rows[0].id]
  );

  res.json({ ok: true, hours: hours.toFixed(2) });
});

// ======================================================================
// PDF – MIT START & ENDE
// ======================================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);
  const employeeName = emp.rows[0].name;

  const head = await pool.query(
    `SELECT customer_name, internal_po
     FROM staffplan
     WHERE calendar_week=$1 AND customer_po=$2 AND employee_name=$3
     LIMIT 1`,
    [kw, customerPo, employeeName]
  );

  const customerName = head.rows[0]?.customer_name || "-";
  const internalPo = head.rows[0]?.internal_po || "-";

  const entries = await pool.query(
    `SELECT work_date, start_time, end_time, total_hours, activity
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date, start_time`,
    [employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // Logo
  if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
    const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
    doc.image(fs.readFileSync(LOGO_FILE), 200, 25, { width: 200 });
  }

  doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });

  doc.fontSize(9).font("Helvetica");
  doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`KW: ${kw}`, 40, 170);
  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  let y = 195;
  const rowH = 14;

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Start", 100, y);
  doc.text("Ende", 150, y);
  doc.text("Tätigkeit", 220, y);
  doc.text("Std.", 520, y, { align: "right" });

  y += rowH;
  doc.font("Helvetica");

  let sum = 0;

  for (const r of entries.rows) {
    if (!r.total_hours) continue;
    sum += Number(r.total_hours);

    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text(new Date(r.start_time).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }), 100, y);
    doc.text(new Date(r.end_time).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }), 150, y);
    doc.text(r.activity || "Arbeitszeit", 220, y);
    doc.text(Number(r.total_hours).toFixed(2), 520, y, { align: "right" });
    y += rowH;
  }

  y += 10;
  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 380, y);
  doc.text(sum.toFixed(2), 520, y, { align: "right" });

  doc.end();
});

// ======================================================================
// START
// ======================================================================
migrate().then(() => {
  app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
});
