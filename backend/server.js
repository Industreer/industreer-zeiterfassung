// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (PDF LAYOUT NACH MUSTER)
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
      total_hours NUMERIC(6,2)
    );
  `);
}

// ============================================================
// LOGO STORAGE
// ============================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (_, file, cb) => {
    if (!["image/png", "image/jpeg"].includes(file.mimetype)) {
      return cb(new Error("Nur PNG oder JPG erlaubt"));
    }
    cb(null, true);
  }
});

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
// HEALTH
// ============================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ============================================================
// PDF – STUNDENNACHWEIS NACH MUSTER
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;

  const empRes = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!empRes.rows.length) return res.sendStatus(404);

  const entriesRes = await pool.query(
    "SELECT work_date, total_hours FROM time_entries WHERE employee_id=$1 ORDER BY work_date",
    [employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // ===== LOGO (MITTIG, GROSS)
  if (fs.existsSync(LOGO_FILE)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";

    const logoWidth = 220;
    const x = (doc.page.width - logoWidth) / 2;
    doc.image(buffer, x, 30, { width: logoWidth, format });
    doc.moveDown(5);
  }

  // ===== TITEL
  doc.font("Helvetica-Bold").fontSize(18)
     .text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown(1.5);

  // ===== KOPFDATEN
  doc.font("Helvetica").fontSize(10);
  doc.text(`Mitarbeiter: ${empRes.rows[0].name}`);
  doc.text(`Kalenderwoche: ${kw}`);
  doc.text(`PO: ${po}`);
  doc.moveDown(1.5);

  // ===== TABELLENKOPF
  const startY = doc.y;
  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, startY);
  doc.text("Tätigkeit", 200, startY);
  doc.text("Stunden", 450, startY, { width: 80, align: "right" });

  doc.moveDown(0.5);
  doc.moveTo(40, doc.y).lineTo(550, doc.y).stroke();
  doc.moveDown(0.5);

  // ===== TABELLENINHALT
  doc.font("Helvetica");
  let sum = 0;

  entriesRes.rows.forEach(r => {
    sum += Number(r.total_hours || 0);
    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40);
    doc.text("Arbeitszeit", 200);
    doc.text(`${r.total_hours} Std`, 450, undefined, { width: 80, align: "right" });
    doc.moveDown();
  });

  doc.moveDown();
  doc.font("Helvetica-Bold");
  doc.text("Gesamtstunden:", 350);
  doc.text(`${sum.toFixed(2)} Std`, 450, undefined, { width: 80, align: "right" });

  // ===== UNTERSCHRIFTEN
  doc.moveDown(4);
  doc.font("Helvetica");
  doc.text("Unterschrift Mitarbeiter:", 40);
  doc.text("______________________________", 40);
  doc.moveDown(2);
  doc.text("Unterschrift Kunde:", 40);
  doc.text("______________________________", 40);

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
