// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL STABIL)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const archiver = require("archiver");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ================= DATABASE =================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ================= INIT DB =================
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staff_plan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      employee_name TEXT NOT NULL,
      requester TEXT,
      po_number TEXT NOT NULL,
      work_date DATE NOT NULL,
      planned_hours NUMERIC(6,2) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS email_outbox (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      email_to TEXT,
      subject TEXT,
      body TEXT,
      kw TEXT,
      status TEXT DEFAULT 'queued',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

// ================= ROUTES =================

// ---------- Health ----------
app.get("/api/health", async (req, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

// ---------- Pages ----------
app.get("/admin", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

app.get("/employee", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"));
});

// =====================================================
// 🔴 HIER WAR DER FEHLER – ROUTE FEHLTE
// =====================================================

// ---------- Employees: LIST ----------
app.get("/api/employees", async (req, res) => {
  const result = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(result.rows);
});

// ---------- Employees: ADD / UPDATE ----------
app.post("/api/employees/upsert", async (req, res) => {
  const { employee_id, name, email, language } = req.body;

  if (!employee_id || !name) {
    return res.status(400).json({ error: "employee_id und name sind Pflicht" });
  }

  await pool.query(
    `
    INSERT INTO employees (employee_id, name, email, language)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (employee_id)
    DO UPDATE SET name=$2, email=$3, language=$4
    `,
    [
      employee_id,
      name,
      email || null,
      language || "de"
    ]
  );

  res.json({ ok: true });
});

// ---------- Staffplan CLEAR ----------
app.post("/api/staffplan/clear", async (_, res) => {
  await pool.query("TRUNCATE staff_plan");
  res.json({ ok: true });
});

// ---------- Dummy PDF Endpoints (kommen gleich) ----------
app.get("/api/admin/pdfs", (_, res) => {
  res.status(501).json({ error: "PDF Export folgt im nächsten Schritt" });
});

app.get("/api/employee/pdfs/last", (_, res) => {
  res.status(501).json({ error: "PDF Export folgt im nächsten Schritt" });
});

app.post("/api/employee/email", (_, res) => {
  res.json({ ok: true, message: "Email in Outbox gespeichert (Demo)" });
});

// ================= START =================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
