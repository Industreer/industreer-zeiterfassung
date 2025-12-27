console.log("🔥🔥🔥 SERVER.JS VERSION 2025-DEBUG-EMPLOYEE-TODAY (ROUTING-FIX) 🔥🔥🔥");
/**
 * backend/server.js
 * CLEAN STABLE VERSION – 2025-01
 *
 * Fix:
 * - /api/employee/today steht VOR /api/employee/:id (Routing-Kollision behoben)
 * - planned_hours wird beim Import nur als Zahl gespeichert (verhindert NUMERIC-Fehler)
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

  // Excel-Seriennummer
  if (typeof cell.v === "number" && isFinite(cell.v)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + cell.v * 86400000);
  }

  const t = String(cell.w || cell.v || "").trim();
  if (!t) return null;

  // DD.MM.YYYY
  let m = t.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // Datum irgendwo im Text: "Sa 27.12.2025"
  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // DD.MM. (ohne Jahr) -> heuristisch Jahr bestimmen
  m = t.match(/^(\d{1,2})\.(\d{1,2})\.$/);
  if (m) {
    const today = new Date();
    const y0 = today.getFullYear();
    let guess = new Date(Date.UTC(y0, +m[2] - 1, +m[1]));
    const diffDays = Math.round((guess.getTime() - today.getTime()) / 86400000);
    if (diffDays > 200) guess = new Date(Date.UTC(y0 - 1, +m[2] - 1, +m[1]));
    if (diffDays < -200) guess = new Date(Date.UTC(y0 + 1, +m[2] - 1, +m[1]));
    return guess;
  }

  // "Sa 27.12." -> DD.MM. irgendwo im Text
  m = t.match(/(\d{1,2})\.(\d{1,2})\./);
  if (m) {
    const today = new Date();
    const y0 = today.getFullYear();
    let guess = new Date(Date.UTC(y0, +m[2] - 1, +m[1]));
    const diffDays = Math.round((guess.getTime() - today.getTime()) / 86400000);
    if (diffDays > 200) guess = new Date(Date.UTC(y0 - 1, +m[2] - 1, +m[1]));
    if (diffDays < -200) guess = new Date(Date.UTC(y0 + 1, +m[2] - 1, +m[1]));
    return guess;
  }

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

// ======================================================
// EMPLOYEE – HEUTIGE PROJEKTE (WICHTIG: VOR /:id!)
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) {
      return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    }

    // optionaler Override zum Testen: ?date=YYYY-MM-DD
    const dateOverride = String(req.query.date || "").trim();
    const today = dateOverride || new Date().toISOString().slice(0, 10);

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
        AND work_date = $2
      ORDER BY customer_po, internal_po
      `,
      [employeeId, today]
    );

    return res.json({
      ok: true,
      date: today,
      projects: rows
    });
  } catch (e) {
    console.error("EMPLOYEE TODAY ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// EMPLOYEE – EINZELNER MITARBEITER
// ======================================================
app.get("/api/employee/:id", async (req, res) => {
  const r = await pool.query(
    `SELECT employee_id,name,email,language FROM employees WHERE employee_id=$1`,
    [req.params.id]
  );
  if (!r.rowCount) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});
// ======================================================
// STAFFPLAN IMPORT (robust: Header-Zeile finden + Datum pro Spalte lesen)
// ======================================================
// --- 2) Dates lückenlos pro Spalte bauen (Formel-Zellen ohne cached value abfangen) ---
let firstDateCol = null;
let baseDate = null;

// erstes parsebares Datum in der Header-Zeile suchen
for (let c = startCol; c <= endCol; c++) {
  const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
  const d = parseExcelDate(cell);
  if (d) {
    firstDateCol = c;
    baseDate = d;
    break;
  }
}

if (!baseDate) {
  return res.json({ ok: false, error: "Header-Zeile gefunden, aber kein erstes Datum parsebar" });
}

const dates = [];
let currentBaseDate = baseDate;
let currentBaseCol = firstDateCol;

for (let c = firstDateCol; c <= endCol; c++) {
  const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
  const parsed = parseExcelDate(cell);

  // Wenn später wieder ein echtes Datum auftaucht (neuer Block), Base neu setzen
  if (parsed) {
    currentBaseDate = parsed;
    currentBaseCol = c;
  }

  // Datum für diese Spalte: entweder parsed oder (Base + Offset)
  const d = parsed
    ? parsed
    : new Date(currentBaseDate.getTime() + (c - currentBaseCol) * 86400000);

  dates.push({
    col: c,
    iso: toIsoDate(d),
    cw: "CW" + getISOWeek(d),
  });
}

console.log(
  "📅 HeaderRow:", headerRow + 1,
  "First:", dates[0]?.iso,
  "Last:", dates[dates.length - 1]?.iso,
  "count:", dates.length
);

    if (headerRow === null || bestCnt < 3) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..21)" });
    }

    // --- 2) Dates pro Spalte aus Header lesen ---
    const dates = [];
    for (let c = startCol; c <= endCol; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
      const d = parseExcelDate(cell);
      if (!d) continue;

      dates.push({
        col: c,
        iso: toIsoDate(d),
        cw: "CW" + getISOWeek(d),
      });
    }

    if (!dates.length) {
      return res.json({ ok: false, error: "Datumszeile gefunden, aber keine Datumsspalten parsebar" });
    }

    console.log(
      "📅 HeaderRow:", headerRow + 1,
      "Dates:", dates[0].iso, "…", dates[dates.length - 1].iso,
      "count:", dates.length
    );

    // --- 3) staffplan leeren ---
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // --- 4) wie bisher: Mitarbeiter in Zeile r=5, Schritt 2, Name in Spalte I (c=8) ---
    for (let r = 5; r < 20000; r += 2) {
      const nameCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      if (!nameCell?.v) continue;
      const employeeName = String(nameCell.v).trim();

      // Mitarbeiter suchen / anlegen
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

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = (typeof planRaw === "number" && isFinite(planRaw)) ? planRaw : null;

        if (!proj && plan === null) continue;

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

    return res.json({
      ok: true,
      imported,
      header_row: headerRow + 1,
      date_from: dates[0].iso,
      date_to: dates[dates.length - 1].iso,
      date_cols: dates.length,
    });
  } catch (e) {
    console.error("STAFFPLAN IMPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: staffplan-check (TEMPORARY)
// ======================================================
app.get("/api/debug/staffplan-check", async (req, res) => {
  const employeeId = String(req.query.employee_id || "").trim();
  const date = String(req.query.date || "").trim(); // YYYY-MM-DD

  if (!date) {
    return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });
  }

  const totalOnDate = await pool.query(
    `SELECT COUNT(*)::int AS cnt FROM staffplan WHERE work_date = $1::date`,
    [date]
  );

  let forEmployee = null;
  let employeeName = null;
  let byName = null;

  if (employeeId) {
    forEmployee = await pool.query(
      `SELECT COUNT(*)::int AS cnt FROM staffplan WHERE work_date = $1::date AND employee_id = $2`,
      [date, employeeId]
    );

    const emp = await pool.query(
      `SELECT name FROM employees WHERE employee_id = $1`,
      [employeeId]
    );
    employeeName = emp.rowCount ? emp.rows[0].name : null;

    if (employeeName) {
      byName = await pool.query(
        `
        SELECT COUNT(*)::int AS cnt
        FROM staffplan
        WHERE work_date = $1::date
          AND lower(regexp_replace(trim(employee_name), '\\s+', ' ', 'g'))
              = lower(regexp_replace(trim($2), '\\s+', ' ', 'g'))
        `,
        [date, employeeName]
      );
    }
  }

  res.json({
    ok: true,
    date,
    total_on_date: totalOnDate.rows[0].cnt,
    employee_id: employeeId || null,
    staffplan_for_employee_id: forEmployee ? forEmployee.rows[0].cnt : null,
    employee_name_from_employees: employeeName,
    staffplan_for_employee_name: byName ? byName.rows[0].cnt : null
  });
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
