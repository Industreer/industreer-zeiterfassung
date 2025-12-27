console.log("🔥🔥🔥 SERVER.JS FULL FINAL + AUTO FIRST TODAY PROJECT 2025-12-27 🔥🔥🔥");
/**
 * backend/server.js
 * Industreer Zeiterfassung – FULL (1:1 tauschbar)
 *
 * Features:
 * - Node/Express + Postgres (Render)
 * - Admin: Logo upload, Staffplan Excel Import
 * - Employee: today projects, time start/end, breaks
 * - Robust Staffplan Import:
 *    - Header Row (Datum) automatisch finden
 *    - Formeln ohne cached value: Datumslücken per BaseDate + Offset füllen
 *    - Requester (Ansprechpartner) aus Spalte I (c=8)
 *    - Mitarbeitername aus Spalte K (c=10), inkl. "Nachname, Vorname" -> "Vorname Nachname"
 * - Routing Fix: /api/employee/today VOR /api/employee/:id
 * - Auto Pick: Wenn Mitarbeiter bei Start/End kein Projekt auswählt -> erstes heutiges Staffplan-Projekt wird gesetzt
 * - Debug Endpoints:
 *    /api/debug/build
 *    /api/debug/staffplan-check?employee_id=1001&date=YYYY-MM-DD
 *    /api/debug/staffplan-rows?date=YYYY-MM-DD&employee_id=1001
 *    /api/debug/staffplan-topdates
 *    /api/debug/time-entries?employee_id=1001
 *    /api/debug/cleanup-time  (löscht alte NULL-Projekt Test-Einträge + schließt offene/kaputte)
 */

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit"); // später
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
// UPLOAD
// ======================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================
// HELPERS
// ======================================================
function toIsoDate(d) {
  return new Date(d).toISOString().slice(0, 10);
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function normalizeSpaces(s) {
  return String(s || "")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeEmployeeName(nameRaw) {
  const t = normalizeSpaces(nameRaw);
  if (!t) return "";
  // "Irrgang, Jens" -> "Jens Irrgang"
  const m = t.match(/^([^,]+),\s*(.+)$/);
  if (m) return normalizeSpaces(`${m[2]} ${m[1]}`);
  return t;
}

/**
 * Excel Datum parsing:
 * - cell.v number: Excel serial date
 * - strings: DD.MM.YYYY oder irgendwo im Text
 * - falls nur DD.MM. -> heuristisch Jahr (nahe heute)
 */
function parseExcelDate(cell) {
  if (!cell) return null;

  if (typeof cell.v === "number" && isFinite(cell.v)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + cell.v * 86400000);
  }

  const t = normalizeSpaces(cell.w || cell.v || "");
  if (!t) return null;

  let m = t.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // DD.MM. ohne Jahr
  m = t.match(/(\d{1,2})\.(\d{1,2})\./);
  if (m) {
    const today = new Date();
    const y0 = today.getUTCFullYear();
    let guess = new Date(Date.UTC(y0, +m[2] - 1, +m[1]));
    const diffDays = Math.round((guess.getTime() - today.getTime()) / 86400000);
    if (diffDays > 200) guess = new Date(Date.UTC(y0 - 1, +m[2] - 1, +m[1]));
    if (diffDays < -200) guess = new Date(Date.UTC(y0 + 1, +m[2] - 1, +m[1]));
    return guess;
  }

  return null;
}

async function ensureTimeEntriesSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      start_time TIMESTAMPTZ,
      end_time TIMESTAMPTZ,
      break_minutes INT DEFAULT 0,
      auto_break_minutes INT DEFAULT 0,
      total_hours NUMERIC,
      overtime_hours NUMERIC,
      activity TEXT,
      internal_po TEXT,
      project_short TEXT,
      requester_name TEXT,
      customer_po TEXT
    );
  `);

  // add columns if missing (safe)
  const cols = [
    ["start_time", "TIMESTAMPTZ"],
    ["end_time", "TIMESTAMPTZ"],
    ["break_minutes", "INT DEFAULT 0"],
    ["auto_break_minutes", "INT DEFAULT 0"],
    ["total_hours", "NUMERIC"],
    ["overtime_hours", "NUMERIC"],
    ["activity", "TEXT"],
    ["internal_po", "TEXT"],
    ["project_short", "TEXT"],
    ["requester_name", "TEXT"],
    ["customer_po", "TEXT"],
  ];

  for (const [name, type] of cols) {
    await pool.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='public' AND table_name='time_entries' AND column_name='${name}'
        ) THEN
          ALTER TABLE public.time_entries ADD COLUMN ${name} ${type};
        END IF;
      END $$;
    `);
  }
}

async function ensureBreakEntriesSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS break_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ
    );
  `);

  const cols = [
    ["start_ts", "TIMESTAMPTZ NOT NULL DEFAULT NOW()"],
    ["end_ts", "TIMESTAMPTZ"],
  ];

  for (const [name, type] of cols) {
    await pool.query(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns
          WHERE table_schema='public' AND table_name='break_entries' AND column_name='${name}'
        ) THEN
          ALTER TABLE public.break_entries ADD COLUMN ${name} ${type};
        END IF;
      END $$;
    `);
  }
}

async function pickFirstTodayProject(employeeId, isoDate) {
  const r = await pool.query(
    `
    SELECT customer_po, internal_po, project_short, requester_name
    FROM staffplan
    WHERE employee_id=$1 AND work_date=$2::date
    ORDER BY customer_po, internal_po
    LIMIT 1
    `,
    [employeeId, isoDate]
  );
  return r.rowCount ? r.rows[0] : null;
}

async function sumBreakMinutes(employeeId, startTs, endTs) {
  await ensureBreakEntriesSchema();

  // close any open breaks at endTs (optional safety)
  await pool.query(
    `UPDATE break_entries SET end_ts=$2 WHERE employee_id=$1 AND end_ts IS NULL`,
    [employeeId, endTs]
  );

  const r = await pool.query(
    `
    SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts))/60),0)::int AS minutes
    FROM break_entries
    WHERE employee_id=$1
      AND end_ts IS NOT NULL
      AND start_ts >= $2
      AND end_ts <= $3
    `,
    [employeeId, startTs, endTs]
  );
  return r.rows[0]?.minutes || 0;
}

// ======================================================
// MIGRATE
// ======================================================
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

  // staffplan IMMER frisch (wie gewünscht)
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

  await ensureTimeEntriesSchema();
  await ensureBreakEntriesSchema();

  console.log("✅ DB migrate finished");
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
// DEBUG BUILD
// ======================================================
app.get("/api/debug/build", (req, res) => {
  res.json({
    ok: true,
    build: "server.js FULL FINAL + AUTO FIRST TODAY PROJECT 2025-12-27",
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
    const employeeId = normalizeSpaces(req.query.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const dateOverride = normalizeSpaces(req.query.date || "");
    const day = dateOverride || toIsoDate(new Date());

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
      [employeeId, day]
    );

    res.json({ ok: true, date: day, projects: rows });
  } catch (e) {
    console.error("EMPLOYEE TODAY ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
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
// STAFFPLAN IMPORT
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    // 1) HeaderRow finden: Scan erste Zeilen, wo viele Datumszellen sind
    let headerRow = null;
    let bestCnt = 0;

    const scanRowMax = 25;     // 0..24
    const scanColMin = 5;      // ab F
    const scanColMax = 260;    // großzügig

    for (let r = 0; r < scanRowMax; r++) {
      let cnt = 0;
      for (let c = scanColMin; c <= scanColMax; c++) {
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
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..25)" });
    }

    // 2) Erstes Datum + erste Datumsspalte finden
    let firstDateCol = null;
    let baseDate = null;
    for (let c = scanColMin; c <= scanColMax; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
      const d = parseExcelDate(cell);
      if (d) {
        firstDateCol = c;
        baseDate = d;
        break;
      }
    }

    if (!baseDate || firstDateCol === null) {
      return res.json({ ok: false, error: "Header-Zeile gefunden, aber kein erstes Datum parsebar" });
    }

    // 3) Dates lückenlos bauen (Formeln ohne cached value abfangen)
    //    -> wir bauen FIX 300 Tage (wie deine alte Version)
    const dates = [];
    for (let i = 0; i < 300; i++) {
      const d = new Date(baseDate.getTime() + i * 86400000);
      dates.push({
        col: firstDateCol + i,
        iso: toIsoDate(d),
        cw: "CW" + getISOWeek(d),
      });
    }

    console.log(
      "📅 HeaderRow:", headerRow + 1,
      "Base:", toIsoDate(baseDate),
      "FirstCol:", firstDateCol,
      "First:", dates[0]?.iso,
      "Last:", dates[dates.length - 1]?.iso,
      "count:", dates.length,
      "dateCellsFoundInHeader:", bestCnt
    );

    // 4) staffplan leeren
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // 5) Mitarbeiter: Requester in Spalte I (c=8), Mitarbeitername in Spalte K (c=10)
    //    Datensätze: wie bisher r=5.., Schritt 2 Zeilen
    for (let r = 5; r < 20000; r += 2) {
      // Requester (Ansprechpartner)
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const requesterName = requesterCell?.v ? normalizeSpaces(requesterCell.v) : null;

      // Mitarbeitername in Spalte K
      const employeeCell = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      if (!employeeCell?.v) continue;

      const employeeName = normalizeEmployeeName(employeeCell.v);
      if (!employeeName) continue;

      // employee_id suchen / anlegen (match auf employees.name = "Vorname Nachname")
      const emp = await pool.query(
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
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          `,
          [employeeId, employeeName, requesterName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
        );

        imported++;
      }
    }

    res.json({
      ok: true,
      imported,
      header_row: headerRow + 1,
      date_from: dates[0].iso,
      date_to: dates[dates.length - 1].iso,
      date_cols: dates.length,
    });
  } catch (e) {
    console.error("STAFFPLAN IMPORT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: CURRENT RUNNING
// ======================================================
app.get("/api/time/current/:employeeId", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    const employeeId = normalizeSpaces(req.params.employeeId || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT start_time
      FROM time_entries
      WHERE employee_id=$1 AND end_time IS NULL AND start_time IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.json({ ok: false });
    res.json({ ok: true, start_time: r.rows[0].start_time });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: START (auto pick project if missing)
// ======================================================
app.post("/api/time/start", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = normalizeSpaces(req.body.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const workDate = toIsoDate(now);

    // Safety: close any open entries
    await pool.query(
      `UPDATE time_entries SET end_time=start_time WHERE employee_id=$1 AND end_time IS NULL AND start_time IS NOT NULL`,
      [employeeId]
    );

    // If frontend does not send a project -> pick first today from staffplan
    let project = {
      customer_po: normalizeSpaces(req.body.customer_po || "") || null,
      internal_po: normalizeSpaces(req.body.internal_po || "") || null,
      project_short: normalizeSpaces(req.body.project_short || "") || null,
      requester_name: normalizeSpaces(req.body.requester_name || "") || null,
    };

    let picked = null;
    const hasAny = project.customer_po || project.internal_po || project.project_short || project.requester_name;
    if (!hasAny) {
      picked = await pickFirstTodayProject(employeeId, workDate);
      if (picked) project = picked;
    }

    const ins = await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, start_time, activity, customer_po, internal_po, project_short, requester_name)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING start_time
      `,
      [
        employeeId,
        workDate,
        now.toISOString(),
        normalizeSpaces(req.body.activity || "") || "Arbeitszeit",
        project.customer_po || null,
        project.internal_po || null,
        project.project_short || null,
        project.requester_name || null,
      ]
    );

    res.json({
      ok: true,
      start_time: ins.rows[0].start_time,
      picked_project: picked
        ? {
            customer_po: picked.customer_po || null,
            internal_po: picked.internal_po || null,
            project_short: picked.project_short || null,
            requester_name: picked.requester_name || null,
          }
        : null,
    });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: END (auto pick project if missing, compute break minutes)
// ======================================================
app.post("/api/time/end", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    await ensureBreakEntriesSchema();

    const employeeId = normalizeSpaces(req.body.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const activity = normalizeSpaces(req.body.activity || "") || "Arbeitszeit";
    const now = new Date();
    const workDate = toIsoDate(now);

    // find last open entry
    const open = await pool.query(
      `
      SELECT id, start_time, customer_po, internal_po, project_short, requester_name
      FROM time_entries
      WHERE employee_id=$1 AND end_time IS NULL AND start_time IS NOT NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!open.rowCount) {
      return res.status(400).json({ ok: false, error: "Kein offener Arbeitsblock gefunden" });
    }

    const row = open.rows[0];
    const startTs = new Date(row.start_time);
    const endTs = now;

    // if project fields are empty -> try fill from staffplan
    let customer_po = row.customer_po;
    let internal_po = row.internal_po;
    let project_short = row.project_short;
    let requester_name = row.requester_name;

    const hasProject = customer_po || internal_po || project_short || requester_name;
    if (!hasProject) {
      const picked = await pickFirstTodayProject(employeeId, workDate);
      if (picked) {
        customer_po = picked.customer_po || null;
        internal_po = picked.internal_po || null;
        project_short = picked.project_short || null;
        requester_name = picked.requester_name || null;
      }
    }

    const breakMinutes = await sumBreakMinutes(employeeId, startTs.toISOString(), endTs.toISOString());

    // compute net hours
    const diffSec = Math.max(0, Math.floor((endTs.getTime() - startTs.getTime()) / 1000));
    const netSec = Math.max(0, diffSec - breakMinutes * 60);
    const netHours = (netSec / 3600).toFixed(2);

    await pool.query(
      `
      UPDATE time_entries
      SET end_time=$2,
          activity=$3,
          break_minutes=$4,
          customer_po=$5,
          internal_po=$6,
          project_short=$7,
          requester_name=$8
      WHERE id=$1
      `,
      [row.id, endTs.toISOString(), activity, breakMinutes, customer_po, internal_po, project_short, requester_name]
    );

    res.json({ ok: true, net_hours: netHours, break_minutes: breakMinutes });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// BREAKS
// ======================================================
app.post("/api/break/start", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();
    const employeeId = normalizeSpaces(req.body.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // prevent double-open breaks
    const open = await pool.query(
      `SELECT id FROM break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true, note: "break already running" });

    await pool.query(
      `INSERT INTO break_entries (employee_id, start_ts) VALUES ($1, NOW())`,
      [employeeId]
    );
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();
    const employeeId = normalizeSpaces(req.body.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      UPDATE break_entries
      SET end_ts=NOW()
      WHERE id = (
        SELECT id FROM break_entries
        WHERE employee_id=$1 AND end_ts IS NULL
        ORDER BY id DESC
        LIMIT 1
      )
      RETURNING EXTRACT(EPOCH FROM (end_ts - start_ts))/60 AS minutes
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.status(400).json({ ok: false, error: "Keine offene Pause" });

    const minutes = Math.max(0, Math.round(Number(r.rows[0].minutes || 0)));
    res.json({ ok: true, minutes });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: STAFFPLAN CHECK (counts)
// ======================================================
app.get("/api/debug/staffplan-check", async (req, res) => {
  try {
    const employeeId = normalizeSpaces(req.query.employee_id || "");
    const date = normalizeSpaces(req.query.date || "");
    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

    const totalOnDate = await pool.query(
      `SELECT COUNT(*)::int AS cnt FROM staffplan WHERE work_date=$1::date`,
      [date]
    );

    let forEmployee = null;
    let employeeName = null;
    let byName = null;

    if (employeeId) {
      forEmployee = await pool.query(
        `SELECT COUNT(*)::int AS cnt FROM staffplan WHERE work_date=$1::date AND employee_id=$2`,
        [date, employeeId]
      );

      const emp = await pool.query(`SELECT name FROM employees WHERE employee_id=$1`, [employeeId]);
      employeeName = emp.rowCount ? emp.rows[0].name : null;

      if (employeeName) {
        byName = await pool.query(
          `
          SELECT COUNT(*)::int AS cnt
          FROM staffplan
          WHERE work_date=$1::date
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
// DEBUG: STAFFPLAN ROWS
// ======================================================
app.get("/api/debug/staffplan-rows", async (req, res) => {
  try {
    const date = normalizeSpaces(req.query.date || "");
    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

    const employeeId = normalizeSpaces(req.query.employee_id || "");

    const q = employeeId
      ? `
        SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
        FROM staffplan
        WHERE work_date=$1::date AND employee_id=$2
        ORDER BY customer_po, internal_po
      `
      : `
        SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
        FROM staffplan
        WHERE work_date=$1::date
        ORDER BY employee_id, customer_po, internal_po
        LIMIT 200
      `;

    const args = employeeId ? [date, employeeId] : [date];
    const r = await pool.query(q, args);
    res.json({ ok: true, date, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: STAFFPLAN TOP DATES
// ======================================================
app.get("/api/debug/staffplan-topdates", async (req, res) => {
  try {
    const r = await pool.query(
      `
      SELECT work_date, COUNT(*)::int AS cnt
      FROM staffplan
      GROUP BY work_date
      ORDER BY work_date DESC
      LIMIT 10
      `
    );
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: TIME ENTRIES
// ======================================================
app.get("/api/debug/time-entries", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    const employeeId = normalizeSpaces(req.query.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT id, work_date, customer_po, internal_po, project_short, requester_name,
             start_time, end_time, activity
      FROM time_entries
      WHERE employee_id=$1
      ORDER BY id DESC
      LIMIT 200
      `,
      [employeeId]
    );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: CLEANUP TIME (löscht alte NULL-Projekt Tests + schließt offene/kaputte)
// ======================================================
app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    await ensureBreakEntriesSchema();

    const employeeId = normalizeSpaces(req.body.employee_id || "");
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // 1) kaputte rows (start_time fehlt)
    const delNullStart = await pool.query(
      `DELETE FROM time_entries WHERE employee_id=$1 AND start_time IS NULL`,
      [employeeId]
    );

    // 2) offene rows schließen (end_time fehlt)
    const closeOpen = await pool.query(
      `UPDATE time_entries SET end_time=start_time
       WHERE employee_id=$1 AND start_time IS NOT NULL AND end_time IS NULL`,
      [employeeId]
    );

    // 3) offene breaks schließen
    const closeBreaks = await pool.query(
      `UPDATE break_entries SET end_ts=start_ts
       WHERE employee_id=$1 AND end_ts IS NULL`,
      [employeeId]
    );

    // 4) alte abgeschlossene Test-rows ohne Projektfelder löschen
    const delClosedNullProject = await pool.query(
      `DELETE FROM time_entries
       WHERE employee_id=$1
         AND end_time IS NOT NULL
         AND customer_po IS NULL
         AND internal_po IS NULL
         AND project_short IS NULL
         AND requester_name IS NULL`,
      [employeeId]
    );

    res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: delNullStart.rowCount,
      closed_open_time_entries: closeOpen.rowCount,
      closed_open_breaks: closeBreaks.rowCount,
      deleted_closed_null_project: delClosedNullProject.rowCount,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// (OPTIONAL) PDF PLACEHOLDERS – später ausbauen
// ======================================================
// Du hast schon Buttons im employee.html.
// Diese Endpoints kannst du später erweitern.
app.get("/api/employee/:id/pos/:kw", async (req, res) => {
  return res.json({ ok: false, error: "Not implemented yet" });
});

app.get("/api/pdf/timesheet/:id/:kw/:po", async (req, res) => {
  return res.status(501).send("Not implemented yet");
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
