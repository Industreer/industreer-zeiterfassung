// ======================================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL)
// - Logo Upload & Anzeige
// - Staffplan Excel Import (robuste Datumserkennung)
// - Zeitbuchung
// - PDF Stundennachweis (Kunde + interne PO im Kopf)
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
      activity TEXT DEFAULT 'Arbeitszeit'
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
// UPLOAD SETUP
// ======================================================================
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }
});

// ======================================================================
// LOGO
// ======================================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false });

  if (!["image/png", "image/jpeg"].includes(req.file.mimetype)) {
    return res.status(400).json({ ok: false, error: "Nur PNG oder JPG erlaubt" });
  }

  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(LOGO_META, JSON.stringify({ mimetype: req.file.mimetype }));
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE) || !fs.existsSync(LOGO_META)) {
    return res.sendStatus(404);
  }
  const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ======================================================================
// HEALTH
// ======================================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ======================================================================
// HELPER: DATUM ROBUST PARSEN
// ======================================================================
function parseExcelDate(value) {
  if (!value) return null;

  if (value instanceof Date && !isNaN(value)) {
    return value.toISOString().slice(0, 10);
  }

  if (typeof value === "number") {
    const ms = Math.round((value - 25569) * 86400 * 1000);
    const d = new Date(ms);
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  if (typeof value === "string") {
    const m = value.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
    if (m) {
      return `${m[3]}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
    }
  }

  return null;
}

// ======================================================================
// STAFFPLAN IMPORT (ROBUST)
// ======================================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Keine Datei" });
    }

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const calendarWeek = ws["L2"] ? String(ws["L2"].v).trim() : null;

    // Datumszeile automatisch finden
    let dates = [];
    let headerRow = null;

    for (let r = 1; r <= 15; r++) {
      const found = [];
      for (let c = 11; c < 60; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: r - 1, c })];
        const iso = cell ? parseExcelDate(cell.v) : null;
        if (iso) found.push({ c, iso });
      }
      if (found.length >= 3) {
        dates = found;
        headerRow = r;
        break;
      }
    }

    if (!dates.length) {
      return res.status(400).json({
        ok: false,
        error: "Keine Datumszeile gefunden (Datum muss sichtbar sein)"
      });
    }

    if (calendarWeek) {
      await pool.query("DELETE FROM staffplan WHERE calendar_week=$1", [
        calendarWeek
      ]);
    }

    let imported = 0;

    for (let row = 6; row < 5000; row++) {
      const customerName = ws[`A${row}`]?.v?.toString().trim() || "";
      const internalPo = ws[`B${row}`]?.v?.toString().trim() || "";
      const customerPo = ws[`E${row}`]?.v?.toString().trim() || "";
      const employeeName = ws[`I${row}`]?.v?.toString().trim() || "";

      if (!employeeName) continue;

      for (const d of dates) {
        const hoursCell =
          ws[XLSX.utils.encode_cell({ r: row - 1, c: d.c })];
        const hours = Number(hoursCell?.v);

        if (!isFinite(hours) || hours <= 0) continue;

        await pool.query(
          `INSERT INTO staffplan
           (calendar_week, employee_name, work_date, customer_name, customer_po, internal_po)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [
            calendarWeek,
            employeeName,
            d.iso,
            customerName,
            customerPo,
            internalPo
          ]
        );

        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, imported });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================================
// ZEITBUCHUNG
// ======================================================================
app.post("/api/time/start", async (req, res) => {
  const { employee_id } = req.body;
  const today = new Date().toISOString().slice(0, 10);

  await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())`,
    [employee_id, today]
  );

  res.json({ ok: true });
});

app.post("/api/time/end", async (req, res) => {
  const { employee_id, activity } = req.body;
  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );

  if (!r.rows.length) return res.status(400).json({ ok: false });

  const start = new Date(r.rows[0].start_time);
  const end = new Date();
  const hours = (end - start) / 3600000;

  await pool.query(
    `UPDATE time_entries
     SET end_time=NOW(), total_hours=$1, activity=$2
     WHERE id=$3`,
    [hours, activity || "Arbeitszeit", r.rows[0].id]
  );

  res.json({ ok: true });
});

// ======================================================================
// PDF STUNDENNACHWEIS
// ======================================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);
  const name = emp.rows[0].name;

  const head = await pool.query(
    `SELECT customer_name, internal_po
     FROM staffplan
     WHERE calendar_week=$1 AND customer_po=$2 AND employee_name=$3
     LIMIT 1`,
    [kw, customerPo, name]
  );

  const customerName = head.rows[0]?.customer_name || "-";
  const internalPo = head.rows[0]?.internal_po || "-";

  const entries = await pool.query(
    `SELECT work_date, total_hours, activity
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date`,
    [employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  if (fs.existsSync(LOGO_FILE)) {
    const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
    doc.image(fs.readFileSync(LOGO_FILE), 200, 25, {
      width: 200,
      format: meta.mimetype === "image/png" ? "PNG" : "JPEG"
    });
  }

  doc.fontSize(16).font("Helvetica-Bold").text("STUNDENNACHWEIS", 0, 110, {
    align: "center"
  });

  doc.fontSize(9).font("Helvetica");
  doc.text(`Mitarbeiter: ${name}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`Kalenderwoche: ${kw}`, 40, 170);
  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  let y = 200;
  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Tätigkeit", 200, y);
  doc.text("Std.", 520, y, { align: "right" });
  y += 15;

  doc.font("Helvetica");
  let sum = 0;

  for (const r of entries.rows) {
    const h = Number(r.total_hours || 0);
    sum += h;
    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text(r.activity || "Arbeitszeit", 200, y);
    doc.text(h.toFixed(2), 520, y, { align: "right" });
    y += 14;
  }

  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 380, y + 10);
  doc.text(sum.toFixed(2), 520, y + 10, { align: "right" });

  doc.end();
});

// ======================================================================
// START
// ======================================================================
migrate().then(() => {
  app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
});
