console.log("🔥🔥🔥 SERVER.JS FULL FINAL + AUTO FIRST TODAY PROJECT + BACKFILL + UPSERT EMPLOYEES 2025-12-27 🔥🔥🔥");
/**
 * backend/server.js
 *
 * ✅ Fixes / Features:
 * - /api/employee/today steht VOR /api/employee/:id (Routing-Kollision)
 * - Staffplan Import robust: Datums-Header finden + lückenlose Dates (Formel-Zellen ohne cached value)
 * - planned_hours wird nur als Zahl gespeichert (verhindert NUMERIC-Fehler)
 * - requester_name aus Excel-Spalte I (c=8), employee_name aus Spalte K (c=10), employee_id aus Spalte J (c=9)
 * - employees INSERT als UPSERT (kein duplicate key crash)
 * - Zeiterfassung: /api/time/start pickt automatisch erstes heutiges Projekt, wenn nichts übergeben wird
 * - /api/time/end macht Backfill, wenn Projektfelder im offenen Eintrag noch NULL sind
 * - Debug-Endpunkte: build, staffplan-check, staffplan-rows, time-entries, cleanup-time, repair-time-schema, backfill-projects
 */

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit"); // optional (PDF später)
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 10000;
const BUILD_ID = "server.js FULL FINAL + AUTO FIRST TODAY PROJECT + BACKFILL + UPSERT EMPLOYEES 2025-12-27";

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
  ssl: process.env.DATABASE_URL?.includes("render.com") ? { rejectUnauthorized: false } : undefined,
});

// ======================================================
// HELPERS
// ======================================================
function toIsoDate(d) {
  return d.toISOString().slice(0, 10);
}

function berlinIsoDate(date = new Date()) {
  // "sv-SE" liefert YYYY-MM-DD
  const s = new Intl.DateTimeFormat("sv-SE", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(date);
  return s;
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function parseExcelDate(cell) {
  if (!cell) return null;

  // Excel-Seriennummer (auch aus Formel-cached values)
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

function normName(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

// ======================================================
// MIGRATE / REPAIR SCHEMA
// ======================================================
async function ensureTimeSchema() {
  // time_entries existieren + fehlende Spalten nachziehen (safe)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      internal_po TEXT,
      project_short TEXT,
      requester_name TEXT,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      start_ts TIMESTAMPTZ,
      end_ts TIMESTAMPTZ,
      break_minutes INTEGER DEFAULT 0,
      auto_break_minutes INTEGER DEFAULT 0,
      total_hours NUMERIC,
      overtime_hours NUMERIC,
      activity TEXT
    );
  `);

  // Add missing columns safely (older DBs)
  const alterStmts = [
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS customer_po TEXT`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS internal_po TEXT`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS project_short TEXT`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS requester_name TEXT`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_time TIMESTAMP`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_time TIMESTAMP`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS break_minutes INTEGER DEFAULT 0`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS auto_break_minutes INTEGER DEFAULT 0`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS total_hours NUMERIC`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS overtime_hours NUMERIC`,
    `ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS activity TEXT`,
  ];

  for (const s of alterStmts) {
    await pool.query(s);
  }

  // breaks table (für Raucherpause)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS break_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ
    );
  `);
}

async function migrate() {
  console.log("🔧 DB migrate start");

  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  // staffplan IMMER frisch (wie bei dir)
  await pool.query(`DROP TABLE IF EXISTS staffplan CASCADE`);
  await pool.query(`
    CREATE TABLE staffplan (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      employee_name TEXT NOT NULL,
      requester_name TEXT,
      work_date DATE NOT NULL,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      internal_po TEXT,
      customer_po TEXT,
      project_short TEXT,
      planned_hours NUMERIC
    );
  `);

  await ensureTimeSchema();

  console.log("✅ DB migrate finished");
}

// ======================================================
// UPLOAD
// ======================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================
// STATIC
// ======================================================
app.use(express.static(FRONTEND_DIR));
app.get("/", (req, res) => res.redirect("/admin"));
app.get("/admin", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "admin.html")));
app.get("/employee", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "employee.html")));

// ======================================================
// HEALTH / BUILD
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));
app.get("/api/debug/build", (req, res) => {
  res.json({
    ok: true,
    build: BUILD_ID,
    node: process.version,
    now: new Date().toISOString(),
  });
});

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
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // optionaler Override zum Testen: ?date=YYYY-MM-DD
    const dateOverride = String(req.query.date || "").trim();
    const today = dateOverride || berlinIsoDate();

    const { rows } = await pool.query(
      `
      SELECT
        work_date,
        calendar_week,
        customer,
        requester_name,
        internal_po,
        customer_po,
        project_short,
        planned_hours
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2::date
      ORDER BY customer_po, internal_po
      `,
      [employeeId, today]
    );

    return res.json({ ok: true, date: today, projects: rows });
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
// STAFFPLAN IMPORT (robust)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.status(400).json({ ok: false, error: "Keine Tabelle gefunden" });

    // --- 1) HeaderRow finden (Zeilen 0..25 scannen, viele Datumszellen = Header)
    const START_COL = 11; // ab L
    const END_COL = 300;

    let headerRow = null;
    let bestCnt = -1;

    for (let r = 0; r <= 25; r++) {
      let cnt = 0;
      for (let c = START_COL; c <= END_COL; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const d = parseExcelDate(cell);
        if (d) cnt++;
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = r;
      }
    }

    if (headerRow === null || bestCnt < 3) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..26)" });
    }

    // --- 2) Dates lückenlos pro Spalte bauen (Formel-Zellen ohne cached value abfangen)
    let firstDateCol = null;
    let baseDate = null;

    for (let c = START_COL; c <= END_COL; c++) {
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

    for (let c = firstDateCol; c <= END_COL; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
      const parsed = parseExcelDate(cell);

      if (parsed) {
        currentBaseDate = parsed;
        currentBaseCol = c;
      }

      const d = parsed
        ? parsed
        : new Date(currentBaseDate.getTime() + (c - currentBaseCol) * 86400000);

      dates.push({
        col: c,
        iso: toIsoDate(d),
        cw: "CW" + getISOWeek(d),
      });
    }

    if (!dates.length) {
      return res.json({ ok: false, error: "Datumszeile gefunden, aber keine Datumsspalten erzeugt" });
    }

    console.log(
      "📅 Staffplan HeaderRow:", headerRow + 1,
      "First:", dates[0]?.iso,
      "Last:", dates[dates.length - 1]?.iso,
      "cols:", dates.length
    );

    // --- 3) staffplan leeren
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // --- 4) Datenzeilen (wie gehabt: ab r=5, Schritt 2)
    // Spalten:
    // - requester_name: I (c=8)
    // - employee_id:    J (c=9)
    // - employee_name:  K (c=10)  -> z.B. "Irrgang, Jens"
    // customer: A (c=0), internal_po: B (c=1), customer_po: E (c=4)
    for (let r = 5; r < 20000; r += 2) {
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      const employeeIdCell = ws[XLSX.utils.encode_cell({ r, c: 9 })];
      const employeeIdRaw = employeeIdCell?.v ?? employeeIdCell?.w ?? null;
      const employeeIdFromExcel = employeeIdRaw !== null ? String(employeeIdRaw).trim() : "";

      const employeeNameCell = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      const employeeName = employeeNameCell?.v ? String(employeeNameCell.v).trim() : "";
      if (!employeeName && !employeeIdFromExcel) continue; // leere Zeile

      // employee_id bestimmen
      let employeeId = employeeIdFromExcel;

      if (!employeeId) {
        // fallback: via Name suchen
        const empByName = await pool.query(`SELECT employee_id FROM employees WHERE name=$1`, [employeeName]);
        if (empByName.rowCount) employeeId = empByName.rows[0].employee_id;
      }

      if (!employeeId) {
        employeeId = "AUTO" + r;
      }

      // UPSERT employees (verhindert duplicate key)
      await pool.query(
        `
        INSERT INTO employees (employee_id, name)
        VALUES ($1, $2)
        ON CONFLICT (employee_id)
        DO UPDATE SET name = EXCLUDED.name
        `,
        [employeeId, employeeName || employeeId]
      );

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = typeof planRaw === "number" && isFinite(planRaw) ? planRaw : null;

        if (!proj && plan === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          `,
          [employeeId, employeeName || "-", requesterName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
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
// STAFFPLAN / TIME DEBUG (Rows)
// ======================================================
app.get("/api/debug/staffplan-rows", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || berlinIsoDate()).trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
      FROM staffplan
      WHERE employee_id = $1 AND work_date = $2::date
      ORDER BY customer_po, internal_po
      `,
      [employeeId, date]
    );

    res.json({ ok: true, date, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/staffplan-check", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || "").trim(); // YYYY-MM-DD

    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

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

      const emp = await pool.query(`SELECT name FROM employees WHERE employee_id = $1`, [employeeId]);
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
      staffplan_for_employee_name: byName ? byName.rows[0].cnt : null,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: get current running entry
// ======================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT start_ts
      FROM time_entries
      WHERE employee_id = $1 AND end_ts IS NULL AND start_ts IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.json({ ok: false });

    res.json({ ok: true, start_time: r.rows[0].start_ts.toISOString() });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: START (auto pick first today project if missing)
// ======================================================
app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const workDate = berlinIsoDate(now);

    // falls schon offen: nicht doppelt starten
    const open = await pool.query(
      `SELECT id, start_ts FROM time_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) {
      return res.json({ ok: true, start_time: open.rows[0].start_ts?.toISOString() || now.toISOString(), note: "already_running" });
    }

    // Projekt-Infos aus Request oder Auto-Pick aus staffplan
    let customer_po = (req.body.customer_po ?? "").toString().trim() || null;
    let internal_po = (req.body.internal_po ?? "").toString().trim() || null;
    let project_short = (req.body.project_short ?? "").toString().trim() || null;
    let requester_name = (req.body.requester_name ?? "").toString().trim() || null;

    let pickedProject = null;

    if (!customer_po && !internal_po && !project_short) {
      const sp = await pool.query(
        `
        SELECT customer_po, internal_po, project_short, requester_name
        FROM staffplan
        WHERE employee_id=$1 AND work_date=$2::date
        ORDER BY customer_po, internal_po
        LIMIT 1
        `,
        [employeeId, workDate]
      );
      if (sp.rowCount) {
        pickedProject = sp.rows[0];
        customer_po = pickedProject.customer_po || null;
        internal_po = pickedProject.internal_po || null;
        project_short = pickedProject.project_short || null;
        requester_name = pickedProject.requester_name || null;
      }
    }

    const activity = String(req.body.activity || "").trim() || "Arbeitszeit";

    const ins = await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, start_ts, start_time, activity, customer_po, internal_po, project_short, requester_name)
      VALUES
        ($1, $2::date, $3, $3, $4, $5, $6, $7, $8)
      RETURNING start_ts
      `,
      [employeeId, workDate, now, activity, customer_po, internal_po, project_short, requester_name]
    );

    return res.json({
      ok: true,
      start_time: ins.rows[0].start_ts ? ins.rows[0].start_ts.toISOString() : now.toISOString(),
      picked_project: pickedProject,
    });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: END (backfill project if missing)
// ======================================================
app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const activity = String(req.body.activity || "").trim() || "Arbeitszeit";

    // offene Pause(n) schließen
    await pool.query(
      `UPDATE break_entries SET end_ts=$2 WHERE employee_id=$1 AND end_ts IS NULL`,
      [employeeId, now]
    );

    // offenen Eintrag holen
    const r = await pool.query(
      `
      SELECT *
      FROM time_entries
      WHERE employee_id=$1 AND end_ts IS NULL AND start_ts IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.status(400).json({ ok: false, error: "Kein offener Arbeitsblock gefunden" });

    const entry = r.rows[0];
    const startTs = entry.start_ts ? new Date(entry.start_ts) : null;
    if (!startTs) return res.status(400).json({ ok: false, error: "Startzeit fehlt im offenen Eintrag" });

    // break minutes in Zeitraum
    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60.0),0)::int AS minutes
      FROM break_entries
      WHERE employee_id=$1
        AND start_ts >= $2
        AND end_ts IS NOT NULL
        AND end_ts <= $3
      `,
      [employeeId, startTs, now]
    );
    const breakMinutes = br.rows[0]?.minutes ?? 0;

    // Arbeitszeit netto
    const totalMinutes = Math.max(0, Math.round((now.getTime() - startTs.getTime()) / 60000));
    const netMinutes = Math.max(0, totalMinutes - breakMinutes);
    const netHours = (netMinutes / 60).toFixed(2);

    // Backfill: falls Projektfelder fehlen, erstes heutiges Projekt nehmen (work_date des Eintrags)
    let filledProject = null;
    const missingProject =
      (entry.customer_po == null && entry.internal_po == null && entry.project_short == null);

    if (missingProject) {
      const wd = entry.work_date ? toIsoDate(new Date(entry.work_date)) : berlinIsoDate();
      const sp = await pool.query(
        `
        SELECT customer_po, internal_po, project_short, requester_name
        FROM staffplan
        WHERE employee_id=$1 AND work_date=$2::date
        ORDER BY customer_po, internal_po
        LIMIT 1
        `,
        [employeeId, wd]
      );
      if (sp.rowCount) {
        filledProject = sp.rows[0];
      }
    }

    await pool.query(
      `
      UPDATE time_entries
      SET end_ts=$2,
          end_time=$2,
          activity=$3,
          break_minutes=$4,
          customer_po = COALESCE(time_entries.customer_po, $5),
          internal_po = COALESCE(time_entries.internal_po, $6),
          project_short = COALESCE(time_entries.project_short, $7),
          requester_name = COALESCE(time_entries.requester_name, $8)
      WHERE id=$1
      `,
      [
        entry.id,
        now,
        activity,
        breakMinutes,
        filledProject ? filledProject.customer_po : null,
        filledProject ? filledProject.internal_po : null,
        filledProject ? filledProject.project_short : null,
        filledProject ? filledProject.requester_name : null,
      ]
    );

    return res.json({
      ok: true,
      net_hours: netHours,
      break_minutes: breakMinutes,
      filled_project: filledProject,
    });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// BREAKS
// ======================================================
app.post("/api/break/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();

    // wenn schon offen: ok
    const open = await pool.query(
      `SELECT id FROM break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true, note: "already_running" });

    await pool.query(
      `INSERT INTO break_entries (employee_id, start_ts) VALUES ($1,$2)`,
      [employeeId, now]
    );
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();

    const r = await pool.query(
      `SELECT id, start_ts FROM break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (!r.rowCount) return res.status(400).json({ ok: false, error: "Keine offene Pause" });

    const start = new Date(r.rows[0].start_ts);
    const minutes = Math.max(0, Math.round((now.getTime() - start.getTime()) / 60000));

    await pool.query(`UPDATE break_entries SET end_ts=$2 WHERE id=$1`, [r.rows[0].id, now]);

    res.json({ ok: true, minutes });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: time_entries list
// ======================================================
app.get("/api/debug/time-entries", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const limit = Math.min(200, Math.max(1, parseInt(req.query.limit || "50", 10)));
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT id, work_date, customer_po, internal_po, project_short, requester_name,
             start_time, end_time, activity
      FROM time_entries
      WHERE employee_id=$1
      ORDER BY id DESC
      LIMIT ${limit}
      `,
      [employeeId]
    );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: cleanup-time (close open, delete broken rows)
// ======================================================
app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // delete rows where start is null (broken)
    const del1 = await pool.query(
      `DELETE FROM time_entries WHERE employee_id=$1 AND (start_ts IS NULL AND start_time IS NULL) RETURNING id`,
      [employeeId]
    );

    // close open time entries with missing end
    const now = new Date();
    const closeWork = await pool.query(
      `
      UPDATE time_entries
      SET end_ts = COALESCE(end_ts, $2),
          end_time = COALESCE(end_time, $2)
      WHERE employee_id=$1 AND start_ts IS NOT NULL AND end_ts IS NULL
      RETURNING id
      `,
      [employeeId, now]
    );

    // close open breaks
    const closeBreaks = await pool.query(
      `
      UPDATE break_entries
      SET end_ts = COALESCE(end_ts, $2)
      WHERE employee_id=$1 AND end_ts IS NULL
      RETURNING id
      `,
      [employeeId, now]
    );

    // optional: delete rows that are closed but still have null project (keep as 0 by default)
    const delNullProject = await pool.query(
      `
      DELETE FROM time_entries
      WHERE employee_id=$1
        AND end_time IS NOT NULL
        AND customer_po IS NULL
        AND internal_po IS NULL
        AND project_short IS NULL
        AND requester_name IS NULL
      RETURNING id
      `,
      [employeeId]
    );

    res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: del1.rowCount,
      closed_open_time_entries: closeWork.rowCount,
      closed_open_breaks: closeBreaks.rowCount,
      deleted_closed_null_project: delNullProject.rowCount,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: repair time schema (returns columns)
// ======================================================
app.get("/api/debug/repair-time-schema", async (req, res) => {
  try {
    await ensureTimeSchema();
    const cols = await pool.query(
      `
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_name='time_entries'
      ORDER BY ordinal_position
      `
    );
    res.json({ ok: true, repaired: true, columns: cols.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: backfill projects for day (fills NULL project fields)
// ======================================================
app.post("/api/debug/backfill-projects", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    const date = String(req.body.date || "").trim(); // YYYY-MM-DD
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

    // first project from staffplan
    const sp = await pool.query(
      `
      SELECT customer_po, internal_po, project_short, requester_name
      FROM staffplan
      WHERE employee_id=$1 AND work_date=$2::date
      ORDER BY customer_po, internal_po
      LIMIT 1
      `,
      [employeeId, date]
    );
    if (!sp.rowCount) {
      return res.json({ ok: true, updated: 0, note: "no staffplan project for that date" });
    }

    const p = sp.rows[0];
    const upd = await pool.query(
      `
      UPDATE time_entries
      SET customer_po = COALESCE(customer_po, $3),
          internal_po = COALESCE(internal_po, $4),
          project_short = COALESCE(project_short, $5),
          requester_name = COALESCE(requester_name, $6)
      WHERE employee_id=$1
        AND work_date=$2::date
        AND (customer_po IS NULL AND internal_po IS NULL AND project_short IS NULL)
      RETURNING id
      `,
      [employeeId, date, p.customer_po, p.internal_po, p.project_short, p.requester_name]
    );

    res.json({ ok: true, date, updated: upd.rowCount, project_used: p });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// OPTIONAL: POS list for KW (used by employee.html print)
// ======================================================
app.get("/api/employee/:employee_id/pos/:kw", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    const kw = String(req.params.kw || "").trim();
    if (!employeeId || !kw) return res.status(400).json({ ok: false, error: "employee_id/kw fehlt" });

    const r = await pool.query(
      `
      SELECT DISTINCT customer_po
      FROM staffplan
      WHERE employee_id=$1 AND calendar_week=$2
        AND customer_po IS NOT NULL AND customer_po <> ''
      ORDER BY customer_po
      `,
      [employeeId, kw]
    );

    res.json({ ok: true, pos: r.rows.map((x) => x.customer_po) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// OPTIONAL: PDF placeholder (so no 404 if clicked)
// ======================================================
app.get("/api/pdf/timesheet/:employee_id/:kw/:po", async (req, res) => {
  // Minimal PDF, damit es nicht 404 ist.
  try {
    const { employee_id, kw, po } = req.params;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="timesheet-${employee_id}-${kw}-${po}.pdf"`);

    const doc = new PDFDocument({ margin: 40 });
    doc.pipe(res);

    doc.fontSize(18).text("INDUSTREER – Stundenzettel (Placeholder)", { align: "left" });
    doc.moveDown();
    doc.fontSize(12).text(`Mitarbeiter-ID: ${employee_id}`);
    doc.text(`Kalenderwoche: ${kw}`);
    doc.text(`Kunden-PO: ${po}`);
    doc.moveDown();
    doc.text("Hinweis: PDF-Ausgabe ist noch nicht final implementiert.");

    doc.end();
  } catch (e) {
    res.status(500).end();
  }
});

// ======================================================
// START
// ======================================================
(async () => {
  try {
    await migrate();
    app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
  } catch (e) {
    console.error("❌ START ERROR:", e);
    process.exit(1);
  }
})();
