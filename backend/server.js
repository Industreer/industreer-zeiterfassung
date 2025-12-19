// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (INTERNE PO AUTOMATISCH)
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
// MIGRATION (STAFFPLAN)
// ============================================================
async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id SERIAL PRIMARY KEY,
      employee_name TEXT,
      work_date DATE,
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

// ============================================================
// LOGO STORAGE
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
// PDF – STUNDENNACHWEIS (INTERNE PO AUTOMATISCH)
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

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
    const w = 200;
    doc.image(buffer, (doc.page.width - w) / 2, 25, { width: w, format });
  }

  // Header
  doc.font("Helvetica-Bold").fontSize(16)
     .text("STUNDENNACHWEIS", 0, 110, { align: "center" });

  doc.font("Helvetica").fontSize(9);
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`, 40, 140);
  doc.text(`Kalenderwoche: ${kw}`, 40, 155);
  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);

  // Tabelle
  let y = 185;
  const rowH = 14;

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Tätigkeit", 170, y);
  doc.text("Interne PO", 300, y);
  doc.text("Std.", 500, y, { align: "right" });
  y += rowH + 4;

  doc.font("Helvetica");
  let sum = 0;

  for (const r of entries.rows) {
    const hours = Number(r.total_hours || 0);
    sum += hours;

    const sp = await pool.query(
      `SELECT internal_po
       FROM staffplan
       WHERE employee_name=$1 AND work_date=$2
       LIMIT 1`,
      [emp.rows[0].name, r.work_date]
    );

    const internalPo = sp.rows[0]?.internal_po || "-";

    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text(r.activity || "Arbeitszeit", 170, y);
    doc.text(internalPo, 300, y);
    doc.text(hours.toFixed(2), 500, y, { align: "right" });

    y += rowH;
  }

  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 350, y);
  doc.text(sum.toFixed(2), 500, y, { align: "right" });

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
