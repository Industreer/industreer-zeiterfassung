// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (LOGO UPLOAD)
// ============================================================

const express = require("express");
const path = require("path");
const fs = require("fs");
const PDFDocument = require("pdfkit");
const multer = require("multer");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ================= MIDDLEWARE =================
app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ================= ROUTING =================
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);

// ================= DATABASE =================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ================= AUTO MIGRATION =================
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
      total_hours NUMERIC(5,2),
      overtime_hours NUMERIC(5,2) DEFAULT 0
    );
  `);
}

// ================= LOGO UPLOAD =================
const upload = multer({
  limits: { fileSize: 5 * 1024 * 1024 }, // 5 MB
  fileFilter: (_, file, cb) => {
    if (!file.mimetype.startsWith("image/")) {
      return cb(new Error("Nur Bilddateien erlaubt"));
    }
    cb(null, true);
  }
});

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  const targetDir = path.join(__dirname, "..", "frontend", "assets");
  const targetFile = path.join(targetDir, "logo.png");

  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
  }

  fs.writeFileSync(targetFile, req.file.buffer);

  res.json({ ok: true });
});

// ================= API =================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ================= START =================
migrate().then(() => {
  app.listen(PORT, () =>
    console.log("🚀 Server läuft auf Port", PORT)
  );
});
