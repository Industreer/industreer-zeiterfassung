// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (CENTERED PDF LOGO)
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
// DB MIGRATION
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
// LOGO STORAGE (FORMAT SAFE)
// ============================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    if (!["image/png", "image/jpeg"].includes(file.mimetype)) {
      return cb(new Error("Nur PNG oder JPG erlaubt"));
    }
    cb(null, true);
  }
});

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(
    LOGO_META,
    JSON.stringify({ mimetype: req.file.mimetype })
  );
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
// PDF TIMESHEET (LOGO CENTERED & LARGE)
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [req.params.employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

  const entries = await pool.query(
    "SELECT work_date, total_hours FROM time_entries WHERE employee_id=$1",
    [req.params.employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // ===== LOGO (ZENTRIERT & GROSS)
  if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";

    const logoWidth = 220;
    const pageWidth = doc.page.width;
    const x = (pageWidth - logoWidth) / 2;

    doc.image(buffer, x, 40, { width: logoWidth, format });
    doc.moveDown(6);
  }

  // ===== TITLE
  doc.fontSize(18).font("Helvetica-Bold")
     .text("STUNDENNACHWEIS", { align: "center" });

  doc.moveDown(1.5);

  doc.fontSize(10).font("Helvetica");
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`);
  doc.text(`Kalenderwoche: ${req.params.kw}`);
  doc.text(`PO: ${req.params.po}`);
  doc.moveDown();

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40);
  doc.text("Arbeitszeit", 200);
  doc.moveDown(0.3);
  doc.font("Helvetica");

  entries.rows.forEach(e => {
    doc.text(new Date(e.work_date).toLocaleDateString("de-DE"), 40);
    doc.text(`${e.total_hours} Std`, 200);
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
