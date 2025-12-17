// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL + MIGRATIONS)
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

// ================= INIT DB (MIGRATION SAFE) =================
async function initDb() {
  // ---- EMPLOYEES BASIS ----
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // 👉 MIGRATIONS FOR EMPLOYEES
  await pool.query(`
    ALTER TABLE employees
    ADD COLUMN IF NOT EXISTS email TEXT;
  `);

  await pool.query(`
    ALTER TABLE employees
    ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'de';
  `);

  // ---- STAFF PLAN BASIS ----
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staff_plan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT NOT NULL,
      employee_name TEXT NOT NULL,
      requester TEXT,
      po_number TEXT NOT NULL,
      work_date DATE NOT NULL,
      planned_hours NUMERIC(6,2) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // 👉 MIGRATION: CUSTOMER
  await pool.query(`
    ALTER TABLE staff_plan
    ADD COLUMN IF NOT EXISTS customer TEXT;
  `);

  // ---- EMAIL OUTBOX ----
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

// ---- HEALTH ----
app.get("/api/health", async (_, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

// ---- PAGES ----
app.get("/admin", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

app.get("/employee", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"));
});

// =====================================================
// EMPLOYEES API
// =====================================================

// LIST
app.get("/api/employees", async (_, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

// UPSERT
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
    DO UPDATE SET
      name = EXCLUDED.name,
      email = EXCLUDED.email,
      language = EXCLUDED.language
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

// ---- STAFFPLAN CLEAR ----
app.post("/api/staffplan/clear", async (_, res) => {
  await pool.query("TRUNCATE staff_plan RESTART IDENTITY");
  res.json({ ok: true });
});

// ---- DEMO PDF / EMAIL (aktiviert später voll) ----
app.get("/api/admin/pdfs", (_, res) => {
  res.json({ ok: true, info: "PDF Export folgt" });
});

app.get("/api/employee/pdfs/last", (_, res) => {
  res.json({ ok: true, info: "Employee PDF folgt" });
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
