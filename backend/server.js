// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (PDF LOGO BUFFER FIX)
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
}

// ============================================================
// LOGO STORAGE (BUFFER SAFE)
// ============================================================
const LOGO_PATH = path.join(__dirname, "logo.png");

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 5 * 1024 * 1024 },
  fileFilter: (_, file, cb) => {
    if (!file.mimetype.startsWith("image/")) {
      return cb(new Error("Only images allowed"));
    }
    cb(null, true);
  }
});

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  fs.writeFileSync(LOGO_PATH, req.file.buffer);
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_PATH)) return res.sendStatus(404);
  res.sendFile(LOGO_PATH);
});

// ============================================================
// HEALTH
// ============================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ============================================================
// TIME TRACKING (gekürzt – unverändert)
// ============================================================
app.post("/api/time/start", async (req, res) => {
  const now = new Date();
  const today = now.toISOString().slice(0, 10);
  await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,$3)`,
    [req.body.employee_id, today, now]
  );
  res.json({ ok: true });
});

app.post("/api/time/end", async (req, res) => {
  const now = new Date();
  const today = now.toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY id DESC LIMIT 1`,
    [req.body.employee_id, today]
  );

  const entry = r.rows[0];
  const hours = (now - new Date(entry.start_time)) / 36e5;

  await pool.query(
    `UPDATE time_entries SET end_time=$1, total_hours=$2 WHERE id=$3`,
    [now, hours, entry.id]
  );

  res.json({ ok: true, totalHours: hours.toFixed(2) });
});

// ============================================================
// PDF TIMESHEET (BUFFER IMAGE – FIX)
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [req.params.employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

  const entries = await pool.query(
    `SELECT work_date, total_hours FROM time_entries
     WHERE employee_id=$1 ORDER BY work_date`,
    [req.params.employeeId]
  );

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // ⭐ LOGO – FINAL FIX
  if (fs.existsSync(LOGO_PATH)) {
    const logoBuffer = fs.readFileSync(LOGO_PATH);
    doc.image(logoBuffer, 40, 30, { width: 120 });
    doc.moveDown(3);
  }

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();
  doc.fontSize(10).text(`Mitarbeiter: ${emp.rows[0].name}`);
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
