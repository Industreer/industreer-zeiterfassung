// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (HYBRID FINAL)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const archiver = require("archiver");
const crypto = require("crypto");
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
  // EMPLOYEES
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL UNIQUE,
      email TEXT,
      language TEXT DEFAULT 'de',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // STAFF PLAN
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

  // EMAIL OUTBOX
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

// ================= HELPERS =================
function autoEmployeeId(name) {
  return "AUTO_" + crypto.createHash("md5").update(name).digest("hex").slice(0, 8);
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

// ================= EMPLOYEES API =================

// LIST
app.get("/api/employees", async (_, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

// UPSERT (ADMIN)
app.post("/api/employees/upsert", async (req, res) => {
  const { employee_id, name, email, language } = req.body;

  if (!employee_id || !name) {
    return res.status(400).json({ error: "employee_id und name sind Pflicht" });
  }

  await pool.query(
    `
    INSERT INTO employees (employee_id, name, email, language)
    VALUES ($1,$2,$3,$4)
    ON CONFLICT (name)
    DO UPDATE SET
      employee_id = EXCLUDED.employee_id,
      email = EXCLUDED.email,
      language = EXCLUDED.language
    `,
    [employee_id, name, email || null, language || "de"]
  );

  res.json({ ok: true });
});

// ================= STAFFPLAN IMPORT (HYBRID) =================
app.post("/api/import/staffplan", async (req, res) => {
  const buffer = Buffer.from(req.body.fileBase64, "base64");
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];

  const calendarWeek = sheet["L2"]?.v;
  if (!calendarWeek) {
    return res.status(400).json({ error: "Kalenderwoche fehlt (Zelle L2)" });
  }

  let imported = 0;

  for (let r = 5; r < 3000; r += 2) {
    const name = sheet[XLSX.utils.encode_cell({ r, c: 8 })]?.v;
    if (!name) break;

    // 🔹 HYBRID: Mitarbeiter automatisch anlegen, falls nicht vorhanden
    const exists = await pool.query(
      "SELECT 1 FROM employees WHERE name=$1",
      [name]
    );

    if (!exists.rows.length) {
      await pool.query(
        `
        INSERT INTO employees (employee_id, name)
        VALUES ($1,$2)
        `,
        [autoEmployeeId(name), name]
      );
    }

    // (Hier würden später die Stunden / Tage folgen)
    imported++;
  }

  res.json({ ok: true, calendarWeek, imported });
});

// ================= START =================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
