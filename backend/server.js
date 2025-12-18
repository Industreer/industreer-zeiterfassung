// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (STABIL + HYBRID + ID-CHANGE)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const crypto = require("crypto");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ---------- DB init + migrations ----------
async function initDb() {
  // Employees base
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Migrations: add missing columns
  await pool.query(`ALTER TABLE employees ADD COLUMN IF NOT EXISTS email TEXT;`);
  await pool.query(`ALTER TABLE employees ADD COLUMN IF NOT EXISTS language TEXT DEFAULT 'de';`);

  // Helpful index for name lookup (case-insensitive lookup in code)
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_employees_name ON employees (name);`);

  // Staff plan table (only used for import + hybrid creation here)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staff_plan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      employee_name TEXT NOT NULL,
      requester TEXT,
      po_number TEXT NOT NULL,
      work_date DATE,
      planned_hours NUMERIC(6,2),
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // Migration: customer column (for older DBs)
  await pool.query(`ALTER TABLE staff_plan ADD COLUMN IF NOT EXISTS customer TEXT;`);
}

function autoEmployeeId(name) {
  return "AUTO_" + crypto.createHash("md5").update(String(name)).digest("hex").slice(0, 8);
}

// ---------- Routes ----------
app.get("/api/health", async (_, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);

app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

// ----- Employees API -----
app.get("/api/employees", async (_, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

// Update by employee_id (normal save)
app.post("/api/employees/update", async (req, res) => {
  const { employee_id, name, email, language } = req.body || {};
  if (!employee_id || !name) {
    return res.status(400).json({ ok: false, error: "employee_id und name sind Pflicht" });
  }

  const r = await pool.query(
    `
    UPDATE employees
    SET name=$2, email=$3, language=$4
    WHERE employee_id=$1
    RETURNING employee_id, name, email, language
    `,
    [String(employee_id).trim(), String(name).trim(), email ?? null, language ?? "de"]
  );

  if (!r.rows.length) {
    return res.status(404).json({ ok: false, error: "Mitarbeiter-ID nicht gefunden" });
  }

  res.json({ ok: true, employee: r.rows[0] });
});

// Create employee (manual)
app.post("/api/employees/create", async (req, res) => {
  const { employee_id, name, email, language } = req.body || {};
  if (!employee_id || !name) {
    return res.status(400).json({ ok: false, error: "employee_id und name sind Pflicht" });
  }

  try {
    const r = await pool.query(
      `
      INSERT INTO employees (employee_id, name, email, language)
      VALUES ($1,$2,$3,$4)
      RETURNING employee_id, name, email, language
      `,
      [String(employee_id).trim(), String(name).trim(), email ?? null, language ?? "de"]
    );
    res.json({ ok: true, employee: r.rows[0] });
  } catch (e) {
    return res.status(400).json({ ok: false, error: "ID existiert bereits" });
  }
});

// Official ID change (controlled)
app.post("/api/employees/change-id", async (req, res) => {
  const { old_id, new_id } = req.body || {};
  if (!old_id || !new_id) {
    return res.status(400).json({ ok: false, error: "old_id und new_id sind Pflicht" });
  }

  const oldId = String(old_id).trim();
  const newId = String(new_id).trim();

  // ensure old exists
  const oldRow = await pool.query(
    "SELECT employee_id FROM employees WHERE employee_id=$1",
    [oldId]
  );
  if (!oldRow.rows.length) {
    return res.status(404).json({ ok: false, error: "old_id nicht gefunden" });
  }

  // ensure new not exists
  const newRow = await pool.query(
    "SELECT employee_id FROM employees WHERE employee_id=$1",
    [newId]
  );
  if (newRow.rows.length) {
    return res.status(400).json({ ok: false, error: "new_id existiert bereits" });
  }

  const r = await pool.query(
    `
    UPDATE employees
    SET employee_id=$2
    WHERE employee_id=$1
    RETURNING employee_id, name, email, language
    `,
    [oldId, newId]
  );

  res.json({ ok: true, employee: r.rows[0] });
});

// ----- Staffplan clear -----
app.post("/api/staffplan/clear", async (_, res) => {
  await pool.query("TRUNCATE staff_plan RESTART IDENTITY");
  res.json({ ok: true });
});

// ----- Staffplan import (HYBRID: auto-create employees by name) -----
app.post("/api/import/staffplan", async (req, res) => {
  try {
    if (!req.body.fileBase64) {
      return res.status(400).json({ ok: false, error: "fileBase64 fehlt" });
    }

    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const wb = XLSX.read(buffer, { type: "buffer" });
    const sheet = wb.Sheets[wb.SheetNames[0]];

    const calendarWeek = sheet["L2"]?.v ? String(sheet["L2"].v).trim() : "";
    if (!calendarWeek) return res.status(400).json({ ok: false, error: "KW fehlt (L2)" });

    let createdEmployees = 0;
    let scanned = 0;

    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })]; // I
      if (!nameCell) break;
      const employeeName = String(nameCell.v || "").trim();
      if (!employeeName) continue;

      scanned++;

      // case-insensitive lookup by name
      const exists = await pool.query(
        "SELECT employee_id FROM employees WHERE LOWER(name)=LOWER($1) LIMIT 1",
        [employeeName]
      );

      if (!exists.rows.length) {
        await pool.query(
          "INSERT INTO employees (employee_id, name, email, language) VALUES ($1,$2,NULL,'de')",
          [autoEmployeeId(employeeName), employeeName]
        );
        createdEmployees++;
      }
    }

    res.json({ ok: true, calendarWeek, scanned, createdEmployees });
  } catch (e) {
    console.error("IMPORT ERROR:", e);
    res.status(500).json({ ok: false, error: "Import fehlgeschlagen" });
  }
});

// ---------- Start ----------
initDb().then(() => {
  // ================= PDF TIMESHEET =================
const PDFDocument = require("pdfkit");

app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;

  // Mitarbeiter laden
  const empRes = await pool.query(
    "SELECT name FROM employees WHERE employee_id = $1",
    [employeeId]
  );

  if (!empRes.rows.length) {
    return res.status(404).send("Mitarbeiter nicht gefunden");
  }

  const employeeName = empRes.rows[0].name;

  // Staffplan-Tage laden (nur Struktur, noch keine IST-Zeiten)
  const planRes = await pool.query(
    `
    SELECT DISTINCT work_date
    FROM staff_plan
    WHERE calendar_week = $1
      AND po_number = $2
      AND employee_name = $3
    ORDER BY work_date
    `,
    [kw, po, employeeName]
  );

  // PDF vorbereiten
  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `inline; filename=Stundennachweis_${employeeName}_${kw}_${po}.pdf`
  );

  doc.pipe(res);

  // ====== HEADER ======
  doc.fontSize(18).text("Stundennachweis", { align: "center" });
  doc.moveDown();

  doc.fontSize(12);
  doc.text(`Mitarbeiter: ${employeeName}`);
  doc.text(`Kalenderwoche: ${kw}`);
  doc.text(`PO: ${po}`);
  doc.moveDown(2);

  // ====== TABLE HEADER ======
  doc.fontSize(11).text("Datum", 50, doc.y, { continued: true });
  doc.text("Arbeitsstunden", 200);
  doc.moveDown();

  let total = 0;

  // ====== TABLE ROWS ======
  planRes.rows.forEach(r => {
    const hours = 8.0; // TESTWERT
    total += hours;

    const d = new Date(r.work_date);
    const dateStr = d.toLocaleDateString("de-DE");

    doc.text(dateStr, 50, doc.y, { continued: true });
    doc.text(hours.toFixed(2), 200);
  });

  doc.moveDown();
  doc.text(`Gesamtstunden: ${total.toFixed(2)}`, { bold: true });

  doc.moveDown(4);
  doc.text("Unterschrift Mitarbeiter: __________________________");
  doc.moveDown(2);
  doc.text("Unterschrift Kunde: ______________________________");

  doc.end();
});
app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
