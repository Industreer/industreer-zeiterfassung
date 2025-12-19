// ======================================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS
// STAFFPLAN IMPORT (MAXIMAL TOLERANT) + LOGO + PDF
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL
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
}

// ======================================================================
// UPLOAD
// ======================================================================
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

// ======================================================================
// LOGO
// ======================================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false });

  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(LOGO_META, JSON.stringify({ mimetype: req.file.mimetype }));
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE)) return res.sendStatus(404);
  const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ======================================================================
// HEALTH
// ======================================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ======================================================================
// HELPER: DATUM & STUNDEN ROBUST PARSEN
// ======================================================================
function parseDateAny(v) {
  if (!v) return null;

  if (v instanceof Date && !isNaN(v)) {
    return v.toISOString().slice(0, 10);
  }

  if (typeof v === "number") {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  if (typeof v === "string") {
    const m = v.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
    if (m) {
      return `${m[3]}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
    }
  }
  return null;
}

function parseHoursAny(v) {
  if (v === null || v === undefined) return null;

  if (typeof v === "number" && isFinite(v)) return v;

  if (typeof v === "string") {
    const m = v.replace(",", ".").match(/(\d+(\.\d+)?)/);
    if (m) return Number(m[1]);
  }
  return null;
}

// ======================================================================
// STAFFPLAN IMPORT (MAXIMAL TOLERANT)
// ======================================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const calendarWeek = ws["L2"] ? String(ws["L2"].v).trim() : null;

    // Datumsspalten finden (ab Spalte L)
    let dates = [];
    for (let r = 1; r <= 15; r++) {
      let found = [];
      for (let c = 11; c < 60; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: r - 1, c })];
        const iso = cell ? parseDateAny(cell.v) : null;
        if (iso) found.push({ c, iso });
      }
      if (found.length >= 3) {
        dates = found;
        break;
      }
    }

    if (!dates.length) {
      return res.json({ ok: true, calendarWeek, imported: 0 });
    }

    if (calendarWeek) {
      await pool.query("DELETE FROM staffplan WHERE calendar_week=$1", [
        calendarWeek
      ]);
    }

    let imported = 0;

    for (let row = 6; row < 6000; row++) {
      const customer_name = ws[`A${row}`]?.v?.toString().trim() || "";
      const internal_po = ws[`B${row}`]?.v?.toString().trim() || "";
      const customer_po = ws[`E${row}`]?.v?.toString().trim() || "";
      const employee_name = ws[`I${row}`]?.v?.toString().trim() || "";

      if (!employee_name) continue;

      for (const d of dates) {
        const cell = ws[XLSX.utils.encode_cell({ r: row - 1, c: d.c })];
        const hours = parseHoursAny(cell?.v);

        if (!hours || hours <= 0) continue;

        await pool.query(
          `INSERT INTO staffplan
           (calendar_week, employee_name, work_date, customer_name, customer_po, internal_po)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [calendarWeek, employee_name, d.iso, customer_name, customer_po, internal_po]
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
// PDF STUNDENNACHWEIS (KOPFDATEN AUS STAFFPLAN)
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

  doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });
  doc.font("Helvetica").fontSize(9);
  doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`Kalenderwoche: ${kw}`, 40, 170);
  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  doc.end();
});

// ======================================================================
// START
// ======================================================================
migrate().then(() => {
  app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
});
