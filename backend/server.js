// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (STAFFPLAN KOPFDATEN)
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
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ============================================================
// PAGES
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
// MIGRATION
// ============================================================
async function migrate() {
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id SERIAL PRIMARY KEY,
      employee_name TEXT,
      work_date DATE,
      customer_po TEXT,
      customer_name TEXT,
      internal_po TEXT
    );
  `);

  // Safety: falls Spalten neu sind
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
}

// ============================================================
// LOGO
// ============================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");
const upload = multer({ storage: multer.memoryStorage() });

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(LOGO_META, JSON.stringify({ mimetype: req.file.mimetype }));
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE)) return res.sendStatus(404);
  const meta = JSON.parse(fs.readFileSync(LOGO_META));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ============================================================
// PDF – STUNDENNACHWEIS (KOPFDATEN AUS STAFFPLAN)
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

  // Kopf-Daten aus Staffplan (einmalig!)
  const staff = await pool.query(
    `SELECT customer_name, internal_po
     FROM staffplan
     WHERE employee_name=$1 AND customer_po=$2
     ORDER BY work_date
     LIMIT 1`,
    [emp.rows[0].name, customerPo]
  );

  const customerName = staff.rows[0]?.customer_name || "-";
  const internalPo = staff.rows[0]?.internal_po || "-";

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

  // Logo
  if (fs.existsSync(LOGO_FILE)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
    doc.image(buffer, (doc.page.width - 200) / 2, 25, { width: 200, format });
  }

  // Header
  doc.font("Helvetica-Bold").fontSize(16)
     .text("STUNDENNACHWEIS", 0, 110, { align: "center" });

  doc.font("Helvetica").fontSize(9);
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`Kalenderwoche: ${kw}`, 40, 170);

  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  // Tabelle
  let y = 195;
  const rowH = 14;

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Tätigkeit", 200, y);
  doc.text("Std.", 500, y, { align: "right" });
  y += rowH + 4;

  doc.font("Helvetica");
  let sum = 0;

  entries.rows.forEach(r => {
    const h = Number(r.total_hours || 0);
    sum += h;

    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text(r.activity || "Arbeitszeit", 200, y);
    doc.text(h.toFixed(2), 500, y, { align: "right" });
    y += rowH;
  });

  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 350, y);
  doc.text(sum.toFixed(2), 500, y, { align: "right" });

  y += 40;
  doc.font("Helvetica");
  doc.text("Unterschrift Mitarbeiter:", 40, y);
  doc.text("__________________________", 40, y + 12);
  doc.text("Unterschrift Kunde:", 300, y);
  doc.text("__________________________", 300, y + 12);

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
