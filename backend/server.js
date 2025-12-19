// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL FINAL)
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
// LOGO UPLOAD (FORMAT SAFE)
// ============================================================
const LOGO_META = path.join(__dirname, "logo.json");
const LOGO_FILE = path.join(__dirname, "logo.bin");

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
// PDF WITH LOGO (FORMAT SAFE)
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

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // ===== LOGO (DER ENTSCHEIDENDE TEIL)
  if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
    doc.image(buffer, 40, 30, { width: 120, format });
    doc.moveDown(3);
  }

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();
  doc.fontSize(10);
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`);
  doc.text(`KW: ${req.params.kw}`);
  doc.text(`PO: ${req.params.po}`);
  doc.moveDown();

  entries.rows.forEach(e => {
    doc.text(
      `${new Date(e.work_date).toLocaleDateString("de-DE")} – ${e.total_hours} Std`
    );
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
