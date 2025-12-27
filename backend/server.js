console.log("🔥🔥🔥 SERVER.JS FULL FINAL + AUTO FIRST TODAY PROJECT 2025-12-27 🔥🔥🔥");
/**
 * backend/server.js
 *
 * FIXES (27.12.2025):
 * - /api/employee/today steht VOR /api/employee/:id (Routing-Kollision)
 * - Staffplan-Import: keine AUTO{row}-IDs mehr -> verhindert "duplicate key violates employees_pkey"
 *   -> Mitarbeiter werden per normalisiertem Namen gefunden oder deterministisch per Hash-ID angelegt (UPSERT)
 * - planned_hours wird nur als Zahl gespeichert (verhindert NUMERIC-Fehler)
 * - Auto: /api/time/start nimmt automatisch das erste heutige Projekt, wenn keine Projekt-Daten gesendet wurden
 *
 * Hinweis:
 * - time_entries wird NICHT gedroppt (persistiert). Wir "reparieren" Schema per ALTER TABLE IF NOT EXISTS.
 * - staffplan wird beim Start neu aufgebaut (DROP + CREATE), wie in deinem bisherigen Setup.
 */

const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit"); // später genutzt
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
  return d.toISOString().slice(0, 10);
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function normalizeSpaces(s) {
  return String(s || "").trim().replace(/\s+/g, " ");
}

function canonicalEmployeeName(nameFromExcel) {
  const s = normalizeSpaces(nameFromExcel);
  // "Nachname, Vorname" -> "Vorname Nachname"
  const m = s.match(/^([^,]+),\s*(.+)$/);
  if (m) return normalizeSpaces(`${m[2]} ${m[1]}`);
  return s;
}

function makeEmployeeIdFromName(canonicalName) {
  const norm = canonicalName.toLowerCase();
  const hash = crypto.createHash("sha1").update(norm).digest("hex").slice(0, 8);
  return `EMP${hash}`;
}

/**
 * Excel Datum robust:
 * - cell.v number => Excel-Seriennummer
 * - cell.w / cell.v string => versucht DD.MM.YYYY oder irgendwo im Text
 * - DD.MM. ohne Jahr => heuristisch Jahr nah an "heute"
 */
function parseExcelDate(cell) {
  if (!cell) return null;

  if (typeof cell.v === "number" && isFinite(cell.v)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + cell.v * 86400000);
  }

  const t = String(cell.w || cell.v || "").trim();
  if (!t) return null;

  let m = t.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  m = t.match(/^(\d{1,2})\.(\d{1,2})\.$/);
  if (m) {
    const today = new Date();
    const y0 = today.getUTCFullYear();
    let guess = new Date(Date.UTC(y0, +m[2] - 1, +m[1]));
    const diffDays = Math.round((guess.getTime() - today.getTime()) / 86400000);
    if (diffDays > 200) guess = new Date(Date.UTC(y0 - 1, +m[2] - 1, +m[1]));
    if (diffDays < -200) guess = new Date(Date.UTC(y0 + 1, +m[2] - 1, +m[1]));
    return guess;
  }

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

function isFiniteNumber(x) {
  return typeof x === "number" && isFinite(x);
}

async function ensureTimeSchema() {
  // time_entries existiert ggf. bereits aus alten Versionen -> wir adden Spalten, ohne zu droppen.
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      internal_po TEXT,
      project_short TEXT,
      requester_name TEXT,
      start_time TIMESTAMPTZ,
      end_time TIMESTAMPTZ,
      break_minutes INTEGER DEFAULT 0,
      auto_break_minutes INTEGER DEFAULT 0,
      total_hours NUMERIC,
      overtime_hours NUMERIC,
      activity TEXT
    );
  `);

  // Falls alte Struktur: fehlende Spalten ergänzen
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS project_short TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS requester_name TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_time TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_time TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS break_minutes INTEGER DEFAULT 0;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS auto_break_minutes INTEGER DEFAULT 0;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS total_hours NUMERIC;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS overtime_hours NUMERIC;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS activity TEXT;`);

  // optional: manche alten Deploys hatten start_ts/end_ts – wir lassen sie existieren, aber nutzen sie NICHT.
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ;`);

  // Breaks-Tabelle (für Raucherpause)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS breaks (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ
    );
  `);
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

  // staffplan IMMER frisch
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
// STATIC
// ======================================================
app.use(express.static(FRONTEND_DIR));
app.get("/", (req, res) => res.redirect("/admin"));
app.get("/admin", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "admin.html")));
app.get("/employee", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "employee.html")));

// ======================================================
// HEALTH + BUILD
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));

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
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const dateOverride = String(req.query.date || "").trim();
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
      ORDER BY customer_po NULLS LAST, internal_po NULLS LAST, id
      `,
      [employeeId, day]
    );

    return res.json({ ok: true, date: day, projects: rows });
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
// Annahme (wie bei dir beobachtet):
// - Mitarbeitername steht in Spalte K (Index 10): "Irrgang, Jens"
// - Requester/Ansprechpartner steht in Spalte I (Index 8): "Hoffmann"
// - Customer (Firma) Spalte A (0)
// - Internal PO Spalte B (1)
// - Customer PO Spalte E (4)
// - Datums-Header: irgendwo in den ersten ~21 Zeilen ab StartCol=11
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const startCol = 11; // wie vorher: ab L
    const endCol = 299;

    // 1) HeaderRow finden: Zeile mit den meisten parsebaren Datumszellen
    let headerRow = null;
    let bestCnt = 0;

    for (let r = 0; r <= 20; r++) {
      let cnt = 0;
      for (let c = startCol; c <= endCol; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const d = parseExcelDate(cell);
        if (d) cnt++;
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = r;
      }
    }

    if (headerRow === null || bestCnt < 1) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..21)" });
    }

    // 2) Dates lückenlos pro Spalte bauen (Formel-Zellen ohne cached value abfangen)
    let firstDateCol = null;
    let baseDate = null;

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
      "📅 HeaderRow:", headerRow + 1,
      "First:", dates[0]?.iso,
      "Last:", dates[dates.length - 1]?.iso,
      "count:", dates.length
    );

    // 3) staffplan leeren
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // 4) Mitarbeiterzeilen (wie vorher): ab r=5 in 2er-Schritten
    for (let r = 5; r < 20000; r += 2) {
      // K = 10 (Mitarbeiter "Nachname, Vorname")
      const empCellK = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      // Fallback (alte Pläne): I = 8
      const empCellI = ws[XLSX.utils.encode_cell({ r, c: 8 })];

      const rawEmployee = String(empCellK?.v || empCellI?.v || "").trim();
      if (!rawEmployee) continue;

      const employeeName = canonicalEmployeeName(rawEmployee);

      // Requester/Ansprechpartner: I = 8 (nur sinnvoll, wenn Mitarbeiter in K steht)
      const requesterRaw =
        empCellK?.v ? ws[XLSX.utils.encode_cell({ r, c: 8 })]?.v : null;
      const requesterName = normalizeSpaces(requesterRaw || "") || null;

      // Mitarbeiter suchen (normalisiert) oder deterministisch anlegen (UPSERT)
      const emp = await pool.query(
        `
        SELECT employee_id, name
        FROM employees
        WHERE lower(regexp_replace(trim(name), '\\s+', ' ', 'g'))
              = lower(regexp_replace(trim($1), '\\s+', ' ', 'g'))
        LIMIT 1
        `,
        [employeeName]
      );

      let employeeId;

      if (emp.rowCount > 0) {
        employeeId = emp.rows[0].employee_id;
      } else {
        employeeId = makeEmployeeIdFromName(employeeName);

        await pool.query(
          `
          INSERT INTO employees (employee_id, name)
          VALUES ($1, $2)
          ON CONFLICT (employee_id) DO UPDATE SET name = EXCLUDED.name
          `,
          [employeeId, employeeName]
        );
      }

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = isFiniteNumber(planRaw) ? planRaw : null;

        if (!proj && plan === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id,employee_name,requester_name,work_date,calendar_week,
             customer,internal_po,customer_po,project_short,planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          `,
          [employeeId, employeeName, requesterName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
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
// TIME: current running entry
// ======================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT start_time
      FROM time_entries
      WHERE employee_id = $1
        AND end_time IS NULL
        AND start_time IS NOT NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.json({ ok: false });
    return res.json({ ok: true, start_time: r.rows[0].start_time });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: start
// Auto-pick first today project if none is provided
// ======================================================
app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    const activity = String(req.body.activity || "Arbeitszeit").trim() || "Arbeitszeit";
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // Wenn bereits offen -> einfach zurückgeben
    const open = await pool.query(
      `
      SELECT id, start_time
      FROM time_entries
      WHERE employee_id = $1
        AND end_time IS NULL
        AND start_time IS NOT NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId]
    );
    if (open.rowCount) {
      return res.json({ ok: true, start_time: open.rows[0].start_time });
    }

    const today = toIsoDate(new Date());

    // Projektinfo aus Request (optional)
    let customer_po = req.body.customer_po ? String(req.body.customer_po) : null;
    let internal_po = req.body.internal_po ? String(req.body.internal_po) : null;
    let project_short = req.body.project_short ? String(req.body.project_short) : null;
    let requester_name = req.body.requester_name ? String(req.body.requester_name) : null;

    let pickedProject = null;

    // Wenn nichts übergeben -> erstes heutiges Projekt aus staffplan nehmen
    if (!customer_po && !internal_po && !project_short) {
      const p = await pool.query(
        `
        SELECT customer_po, internal_po, project_short, requester_name
        FROM staffplan
        WHERE employee_id = $1
          AND work_date = $2::date
        ORDER BY customer_po NULLS LAST, internal_po NULLS LAST, id
        LIMIT 1
        `,
        [employeeId, today]
      );

      if (p.rowCount) {
        customer_po = p.rows[0].customer_po || null;
        internal_po = p.rows[0].internal_po || null;
        project_short = p.rows[0].project_short || null;
        requester_name = p.rows[0].requester_name || null;

        pickedProject = {
          customer_po,
          internal_po,
          project_short,
          requester_name,
        };
      }
    }

    const startTime = new Date();

    await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, customer_po, internal_po, project_short, requester_name, start_time, activity)
      VALUES
        ($1, $2::date, $3, $4, $5, $6, $7, $8)
      `,
      [employeeId, today, customer_po, internal_po, project_short, requester_name, startTime.toISOString(), activity]
    );

    return res.json({
      ok: true,
      start_time: startTime.toISOString(),
      picked_project: pickedProject,
    });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// BREAK: start/end (Raucherpause)
// ======================================================
app.post("/api/break/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true, note: "break already running" });

    const now = new Date().toISOString();
    await pool.query(
      `INSERT INTO breaks (employee_id, start_ts) VALUES ($1, $2)`,
      [employeeId, now]
    );
    res.json({ ok: true, start_ts: now });
  } catch (e) {
    console.error("BREAK START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id, start_ts FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (!open.rowCount) return res.json({ ok: false, error: "keine offene Pause" });

    const now = new Date();
    const start = new Date(open.rows[0].start_ts);
    const minutes = Math.max(0, Math.round((now.getTime() - start.getTime()) / 60000));

    await pool.query(
      `UPDATE breaks SET end_ts=$1 WHERE id=$2`,
      [now.toISOString(), open.rows[0].id]
    );

    res.json({ ok: true, minutes });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: end
// ======================================================
app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    const activity = String(req.body.activity || "Arbeitszeit").trim() || "Arbeitszeit";
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `
      SELECT id, start_time
      FROM time_entries
      WHERE employee_id = $1
        AND end_time IS NULL
        AND start_time IS NOT NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!open.rowCount) {
      return res.json({ ok: false, error: "Kein offener Arbeitsblock gefunden" });
    }

    const id = open.rows[0].id;
    const startTime = new Date(open.rows[0].start_time);
    const endTime = new Date();

    // Break-Minuten innerhalb des Blocks summieren
    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_ts, $2::timestamptz) - start_ts))) ,0) AS sec
      FROM breaks
      WHERE employee_id = $1
        AND start_ts >= $2::timestamptz
        AND start_ts <= $3::timestamptz
      `,
      [employeeId, startTime.toISOString(), endTime.toISOString()]
    );

    const breakSeconds = Number(br.rows[0]?.sec || 0);
    const breakMinutes = Math.max(0, Math.round(breakSeconds / 60));

    const totalSeconds = Math.max(0, Math.floor((endTime.getTime() - startTime.getTime()) / 1000));
    const netSeconds = Math.max(0, totalSeconds - breakMinutes * 60);
    const netHours = (netSeconds / 3600).toFixed(2);

    await pool.query(
      `
      UPDATE time_entries
      SET end_time = $1,
          activity = $2,
          break_minutes = $3,
          total_hours = $4
      WHERE id = $5
      `,
      [endTime.toISOString(), activity, breakMinutes, netHours, id]
    );

    return res.json({
      ok: true,
      net_hours: netHours,
      break_minutes: breakMinutes,
    });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: staffplan-check + staffplan-rows
// ======================================================
app.get("/api/debug/staffplan-check", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || "").trim();

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
    console.error("STAFFPLAN CHECK ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/staffplan-rows", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || "").trim();
    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

    const params = [date];
    let where = `WHERE work_date=$1::date`;

    if (employeeId) {
      params.push(employeeId);
      where += ` AND employee_id=$2`;
    }

    const r = await pool.query(
      `
      SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
      FROM staffplan
      ${where}
      ORDER BY id
      LIMIT 200
      `,
      params
    );

    res.json({ ok: true, date, rows: r.rows });
  } catch (e) {
    console.error("STAFFPLAN ROWS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: time entries list
// ======================================================
app.get("/api/debug/time-entries", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    const params = [];
    let where = "";

    if (employeeId) {
      where = "WHERE employee_id=$1";
      params.push(employeeId);
    }

    const r = await pool.query(
      `
      SELECT id, work_date, customer_po, internal_po, project_short, requester_name,
             start_time, end_time, activity
      FROM time_entries
      ${where}
      ORDER BY id DESC
      LIMIT 200
      `,
      params
    );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("TIME ENTRIES DEBUG ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: cleanup-time (hilft gegen kaputte Testdaten)
// - löscht Einträge mit NULL start_time
// - schließt offene time_entries (end_time NULL) auf start_time
// - schließt offene breaks (end_ts NULL) auf start_ts
// - optional: löscht closed entries ohne Projektzuordnung (falls du willst)
// ======================================================
app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const delNullStart = await pool.query(
      `DELETE FROM time_entries WHERE employee_id=$1 AND start_time IS NULL RETURNING id`,
      [employeeId]
    );

    const closeOpenTime = await pool.query(
      `
      UPDATE time_entries
      SET end_time = start_time
      WHERE employee_id=$1
        AND start_time IS NOT NULL
        AND end_time IS NULL
      RETURNING id
      `,
      [employeeId]
    );

    const closeOpenBreaks = await pool.query(
      `
      UPDATE breaks
      SET end_ts = start_ts
      WHERE employee_id=$1
        AND end_ts IS NULL
      RETURNING id
      `,
      [employeeId]
    );

    // Optional: wenn du wirklich willst: abgeschlossene Einträge ohne Projekt löschen
    const delClosedNullProject = await pool.query(
      `
      DELETE FROM time_entries
      WHERE employee_id=$1
        AND end_time IS NOT NULL
        AND (customer_po IS NULL AND internal_po IS NULL AND project_short IS NULL)
      RETURNING id
      `,
      [employeeId]
    );

    res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: delNullStart.rowCount,
      closed_open_time_entries: closeOpenTime.rowCount,
      closed_open_breaks: closeOpenBreaks.rowCount,
      deleted_closed_null_project: delClosedNullProject.rowCount,
    });
  } catch (e) {
    console.error("CLEANUP TIME ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
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
