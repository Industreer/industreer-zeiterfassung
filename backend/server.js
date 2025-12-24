/**
 * backend/server.js
 * Stable full version – employee_id based
 */

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 10000;

// ======================================================
// Paths
// ======================================================
const ROOT = path.join(__dirname, "..");
const FRONTEND_DIR = path.join(ROOT, "frontend");
const DATA_DIR = path.join(__dirname, "data");
const LOGO_FILE = path.join(DATA_DIR, "logo.png");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ======================================================
// DB
// ======================================================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("render.com")
    ? { rejectUnauthorized: false }
    : undefined,
});
async function migrate() {
  console.log("🔧 DB migrate start");

async function migrate() {
  console.log("🔧 DB migrate start");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id SERIAL PRIMARY KEY
    );
  `);

  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS employee_id TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS employee_name TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS work_date DATE`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS calendar_week TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_name TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_po TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS project_short TEXT`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS planned_hours NUMERIC`);

  console.log("✅ DB migrate finished");
}


// ======================================================
// Upload
// ======================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================
// Helpers
// ======================================================
function toIsoDate(d) {
  return d.toISOString().slice(0, 10);
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function parseExcelDate(cell) {
  if (!cell) return null;
  if (typeof cell.v === "number") {
    const epoch = new Date(1899, 11, 30);
    return new Date(epoch.getTime() + cell.v * 86400000);
  }
  const t = String(cell.w || cell.v || "").trim();
  const m = t.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(+m[3], +m[2] - 1, +m[1]);
  return null;
}

// ======================================================
// MIGRATE
// ======================================================
async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      employee_name TEXT NOT NULL,
      work_date DATE NOT NULL,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      internal_po TEXT,
      customer_po TEXT,
      project_short TEXT,
      planned_hours NUMERIC
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ,
      activity TEXT
    );
  `);
}

// ======================================================
// STATIC
// ======================================================
app.use(express.static(FRONTEND_DIR));
app.get("/", (req, res) => res.redirect("/admin"));
app.get("/admin", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "admin.html")));
app.get("/employee", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "employee.html")));

// ======================================================
// HEALTH
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));

// ======================================================
// LOGO
// ======================================================
app.get("/api/logo", (req, res) => {
  if (!fs.existsSync(LOGO_FILE)) return res.status(404).end();
  res.type("png");
  fs.createReadStream(LOGO_FILE).pipe(res);
});

app.post("/api/logo", upload.single("file"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false });
  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  res.json({ ok: true });
});

// ======================================================
// EMPLOYEES
// ======================================================
app.get("/api/employees", async (req, res) => {
  const r = await pool.query(`SELECT * FROM employees ORDER BY name`);
  res.json(r.rows);
});

app.get("/api/employee/:id", async (req, res) => {
  const r = await pool.query(
    `SELECT employee_id,name,email,language FROM employees WHERE employee_id=$1`,
    [req.params.id]
  );
  if (!r.rowCount) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});// ======================================================================
// EMPLOYEE – HEUTIGE PROJEKTE / DEBUG
// ======================================================================

app.get("/api/employee/today", async (req, res) => {
  console.log("➡️ /api/employee/today aufgerufen");
  console.log("Query:", req.query);

  try {
    const employeeId = req.query.employee_id;
    if (!employeeId) {
      return res.status(400).json({
        ok: false,
        error: "employee_id fehlt"
      });
    }

    const today = new Date();
    const isoToday = today.toISOString().slice(0, 10);

    console.log("employee_id =", employeeId);
    console.log("isoToday =", isoToday);

    const q = `
      SELECT
        work_date,
        calendar_week,
        customer,
        internal_po,
        customer_po,
        project_short,
        planned_hours
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2
    `;

    console.log("SQL läuft…");

    const result = await pool.query(q, [employeeId, isoToday]);

    console.log("SQL rows:", result.rows.length);

    return res.json({
      ok: true,
      rows: result.rows
    });

  } catch (e) {
    console.error("❌ TODAY ERROR:", e);
    return res.status(500).json({
      ok: false,
      error: e.message
    });
  }
});

// ======================================================================
// EMPLOYEE – HEUTIGE PROJEKTE / POs
// ======================================================================

app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = req.query.employee_id;
    if (!employeeId) {
      return res.status(400).json({
        ok: false,
        error: "employee_id fehlt"
      });
    }

    // Heute (lokales Datum, ohne Uhrzeit)
    const today = new Date();
    const isoToday = today.toISOString().slice(0, 10); // YYYY-MM-DD

    // Debug (hilft enorm bei Tests)
    console.log("[TODAY] employee_id =", employeeId, "date =", isoToday);

    const q = `
      SELECT
        work_date,
        calendar_week,
        customer,
        internal_po,
        customer_po,
        project_short,
        planned_hours
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2
      ORDER BY customer_po, internal_po
    `;

    const { rows } = await pool.query(q, [employeeId, isoToday]);

    if (!rows || rows.length === 0) {
      return res.json({
        ok: true,
        date: isoToday,
        projects: [],
        message: "Keine Projekte für heute gefunden"
      });
    }

    // Gruppieren nach PO (falls mehrfach gleiche PO vorkommt)
    const projects = rows.map(r => ({
      date: r.work_date,
      calendarWeek: r.calendar_week,
      customer: r.customer || "",
      customerPo: r.customer_po || "",
      internalPo: r.internal_po || "",
      projectShort: r.project_short || "",
      plannedHours: Number(r.planned_hours || 0)
    }));

    return res.json({
      ok: true,
      date: isoToday,
      projects
    });

  } catch (e) {
    console.error("TODAY ERROR:", e);
    res.status(500).json({
      ok: false,
      error: e.message
    });
  }
});

// ======================================================
// STAFFPLAN IMPORT
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false });

  const wb = XLSX.read(req.file.buffer, { type: "buffer" });
  const ws = wb.Sheets[wb.SheetNames[0]];

  // find first date (row 3+, col L+)
  let startCol = null;
  let baseDate = null;
  for (let c = 11; c < 300; c++) {
    const cell = ws[XLSX.utils.encode_cell({ r: 3, c })];
    const d = parseExcelDate(cell);
    if (d) {
      startCol = c;
      baseDate = d;
      break;
    }
  }
  if (!baseDate) return res.json({ ok: false, error: "Kein Datum gefunden" });

  const dates = [];
  for (let i = 0; i < 300; i++) {
    const d = new Date(baseDate);
    d.setDate(baseDate.getDate() + i);
    dates.push({ col: startCol + i, iso: toIsoDate(d), cw: "CW" + getISOWeek(d) });
  }

  await pool.query("DELETE FROM staffplan");

  let imported = 0;

  for (let r = 5; r < 20000; r += 2) {
    const nameCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
    if (!nameCell?.v) continue;
    const employeeName = String(nameCell.v).trim();

    let emp = await pool.query(`SELECT employee_id FROM employees WHERE name=$1`, [employeeName]);
    let employeeId;
    if (emp.rowCount === 0) {
      employeeId = "AUTO" + r;
      await pool.query(`INSERT INTO employees (employee_id,name) VALUES ($1,$2)`, [employeeId, employeeName]);
    } else {
      employeeId = emp.rows[0].employee_id;
    }

    const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
    const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
    const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

    for (const d of dates) {
      const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;
      const plan = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v || null;
      if (!proj && !plan) continue;

      await pool.query(
        `
        INSERT INTO staffplan
          (employee_id,employee_name,work_date,calendar_week,customer,internal_po,customer_po,project_short,planned_hours)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        `,
        [employeeId, employeeName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
      );
      imported++;
    }
  }

  res.json({ ok: true, imported });
});

// ======================================================
// TODAY PROJECTS (FIXED)
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  const id = req.query.employee_id;
  const today = toIsoDate(new Date());
  const r = await pool.query(
    `SELECT * FROM staffplan WHERE employee_id=$1 AND work_date=$2`,
    [id, today]
  );
  res.json({ ok: true, rows: r.rows });
});
// ======================================================================
// LOGO UPLOAD & ABRUF
// ======================================================================

app.post("/api/logo", upload.single("file"), (req, res) => {
  if (!req.file) {
    return res.status(400).json({ ok: false, error: "Keine Datei erhalten" });
  }
  res.json({ ok: true });
});

app.get("/api/logo", (req, res) => {
  if (!fs.existsSync(LOGO_FILE)) {
    return res.status(404).end();
  }
  res.sendFile(LOGO_FILE);
});

// ======================================================
// START
// ======================================================
(async () => {
  try {
    await migrate();
    app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
  } catch (e) {
    console.error(e);
    process.exit(1);
    await pool.query(`
  ALTER TABLE staffplan
  ADD COLUMN IF NOT EXISTS customer TEXT
`);

  }
})();
