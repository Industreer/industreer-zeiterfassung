// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (STABIL / FINAL B3)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const crypto = require("crypto");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ================= MIDDLEWARE =================
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
      calendar_week TEXT,
      employee_name TEXT,
      po_number TEXT,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

// ================= HELPERS =================
function autoEmployeeId(name) {
  return (
    "AUTO_" +
    crypto.createHash("md5").update(String(name)).digest("hex").slice(0, 8)
  );
}

function getWeekDates(kw) {
  const year = new Date().getFullYear();
  const week = parseInt(kw.replace("CW", ""), 10);
  const simple = new Date(year, 0, 1 + (week - 1) * 7);
  const dow = simple.getDay();
  const monday = new Date(simple);

  if (dow <= 4) {
    monday.setDate(simple.getDate() - simple.getDay() + 1);
  } else {
    monday.setDate(simple.getDate() + 8 - simple.getDay());
  }

  return Array.from({ length: 5 }).map((_, i) => {
    const d = new Date(monday);
    d.setDate(monday.getDate() + i);
    return d;
  });
}

// ================= ROUTES =================

// ---- HEALTH ----
app.get("/api/health", async (_, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

// ---- EMPLOYEES ----
app.get("/api/employees", async (_, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

app.post("/api/employees/update", async (req, res) => {
  const { employee_id, name, email, language } = req.body;

  const r = await pool.query(
    `
    UPDATE employees
    SET name=$2, email=$3, language=$4
    WHERE employee_id=$1
    RETURNING employee_id, name, email, language
    `,
    [employee_id, name, email, language || "de"]
  );

  if (!r.rows.length) {
    return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });
  }

  res.json({ ok: true, employee: r.rows[0] });
});

app.post("/api/employees/change-id", async (req, res) => {
  const { old_id, new_id } = req.body;

  const exists = await pool.query(
    "SELECT 1 FROM employees WHERE employee_id=$1",
    [old_id]
  );
  if (!exists.rows.length) {
    return res.status(404).json({ ok: false, error: "Alte ID nicht gefunden" });
  }

  const conflict = await pool.query(
    "SELECT 1 FROM employees WHERE employee_id=$1",
    [new_id]
  );
  if (conflict.rows.length) {
    return res.status(400).json({ ok: false, error: "Neue ID existiert bereits" });
  }

  const r = await pool.query(
    `
    UPDATE employees
    SET employee_id=$2
    WHERE employee_id=$1
    RETURNING employee_id, name, email, language
    `,
    [old_id, new_id]
  );

  res.json({ ok: true, employee: r.rows[0] });
});

// ---- STAFFPLAN ----
app.post("/api/staffplan/clear", async (_, res) => {
  await pool.query("TRUNCATE staff_plan RESTART IDENTITY");
  res.json({ ok: true });
});

app.post("/api/import/staffplan", async (req, res) => {
  const buffer = Buffer.from(req.body.fileBase64, "base64");
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];

  let createdEmployees = 0;

  for (let r = 5; r < 5000; r += 2) {
    const cell = sheet[XLSX.utils.encode_cell({ r, c: 8 })]; // Spalte I
    if (!cell) break;

    const name = String(cell.v || "").trim();
    if (!name) continue;

    const exists = await pool.query(
      "SELECT 1 FROM employees WHERE LOWER(name)=LOWER($1)",
      [name]
    );

    if (!exists.rows.length) {
      await pool.query(
        "INSERT INTO employees (employee_id, name) VALUES ($1,$2)",
        [autoEmployeeId(name), name]
      );
      createdEmployees++;
    }
  }

  res.json({ ok: true, createdEmployees });
});

// ================= PDF TIMESHEET (B3) =================
app.get("/api/pdf/timesheet/:employeeId/:kw/:po", async (req, res) => {
  const { employeeId, kw, po } = req.params;

  const empRes = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!empRes.rows.length) {
    return res.status(404).send("Mitarbeiter nicht gefunden");
  }

  const employeeName = empRes.rows[0].name;
  const days = getWeekDates(kw);

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  res.setHeader(
    "Content-Disposition",
    `inline; filename=Stundennachweis_${employeeName}_${kw}_${po}.pdf`
  );
  doc.pipe(res);

  // HEADER
  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();

  doc.fontSize(10);
  doc.text(`Name: ${employeeName}`);
  doc.text(`Kalenderwoche: ${kw}`);
  doc.text(`PO / Auftragsnummer: ${po}`);
  doc.moveDown();

  // TABLE HEADER
  doc.font("Helvetica-Bold");
  doc.text("Datum", 40);
  doc.text("Tätigkeit", 150);
  doc.text("Arbeitsstunden", 450, undefined, { align: "right" });
  doc.moveDown(0.5);
  doc.font("Helvetica");

  let total = 0;

  days.forEach(d => {
    const hours = 8.0;
    total += hours;

    doc.text(d.toLocaleDateString("de-DE"), 40);
    doc.text("Montage", 150);
    doc.text(hours.toFixed(2), 450, undefined, { align: "right" });
  });

  doc.moveDown();
  doc.font("Helvetica-Bold");
  doc.text("Gesamtstunden:", 150);
  doc.text(total.toFixed(2), 450, undefined, { align: "right" });
  doc.font("Helvetica");

  doc.moveDown(3);
  doc.text("Ort / Datum: ________________________________");
  doc.moveDown(2);
  doc.text("Unterschrift Mitarbeiter: ________________________________");
  doc.moveDown(2);
  doc.text("Unterschrift Kunde: ________________________________");

  doc.end();
});

// ================= START =================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
