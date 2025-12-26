console.log("🔥🔥🔥 SERVER.JS VERSION 2025-DEBUG-EMPLOYEE-TODAY 🔥🔥🔥");
/**
 * backend/server.js
 * CLEAN STABLE VERSION – 2025-01
 */

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit"); // wird später genutzt
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 10000;

// ======================================================
// PATHS
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

// ======================================================
// MIGRATE (EINMALIG & SAUBER)
// ======================================================
async function migrate() {
  console.log("🔧 DB migrate start");

  // employees bleibt bestehen
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  // staffplan IMMER frisch (wichtig!)
  await pool.query(`DROP TABLE IF EXISTS staffplan CASCADE`);

  await pool.query(`
    CREATE TABLE staffplan (
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

  // time_entries bleibt bestehen
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

  console.log("✅ DB migrate finished");
}
// ======================================================
// UPLOAD
// ======================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================
// HELPERS
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
  if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });
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
});

// ======================================================
// EMPLOYEE – HEUTIGE PROJEKTE
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = req.query.employee_id;
    if (!employeeId) {
      return res.status(400).json({
        ok: false,
        error: "employee_id fehlt"
      });
    }

    const today = toIsoDate(new Date());

    const { rows } = await pool.query(
      `
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
        AND work_date <= $2
      ORDER BY work_date DESC
      `,
      [employeeId, today]
    );

    return res.json({
      ok: true,
      date: today,
      projects: rows
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
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

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
    if (!baseDate) {
      return res.json({ ok: false, error: "Kein Datum gefunden (ab L4)" });
    }

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

      let emp = await pool.query(
        `SELECT employee_id FROM employees WHERE name=$1`,
        [employeeName]
      );

      let employeeId;
      if (emp.rowCount === 0) {
        employeeId = "AUTO" + r;
        await pool.query(
          `INSERT INTO employees (employee_id,name) VALUES ($1,$2)`,
          [employeeId, employeeName]
        );
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
            (employee_id,employee_name,work_date,calendar_week,
             customer,internal_po,customer_po,project_short,planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
          `,
          [employeeId, employeeName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
        );

        imported++;
      }
    }

    res.json({ ok: true, imported });

  } catch (e) {
    console.error("STAFFPLAN IMPORT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// START
// ======================================================
(async () => {
  try {
    await migrate();
    app.listen(PORT, () =>
      console.log("🚀 Server läuft auf Port", PORT)
    );
  } catch (e) {
    console.error("❌ START ERROR:", e);
    process.exit(1);
  }
})();
