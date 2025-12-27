console.log("🔥🔥🔥 SERVER.JS FULL FINAL STABLE 2025-12-27 (NO DROP STAFFPLAN + HYBRID IDs + UPSERT + DEBUG) 🔥🔥🔥");
/**
 * backend/server.js
 * FULL STABLE VERSION – 2025-12-27
 *
 * Fixes:
 * - staffplan wird NICHT mehr beim Start gedroppt (bleibt nach Deploy erhalten)
 * - staffplan wird NUR beim Import geleert (TRUNCATE)
 * - Import: robustes HeaderRow-Finding + Dates lückenlos (Formelzellen ok)
 * - Hybrid-Mitarbeiter: Name-Normalisierung "Nachname, Vorname" -> "Vorname Nachname"
 * - Employees: UPSERT (keine duplicate key employees_pkey mehr)
 * - Debug-Endpoints für schnelle Diagnose
 * - "today" in Europe/Berlin (nicht UTC)
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
  ssl: process.env.DATABASE_URL?.includes("render.com") ? { rejectUnauthorized: false } : undefined,
});

// ======================================================
// UPLOAD
// ======================================================
const upload = multer({ storage: multer.memoryStorage() });

// ======================================================
// HELPERS
// ======================================================
function berlinISODate(d = new Date()) {
  // YYYY-MM-DD in Europe/Berlin, unabhängig vom Server-UTC
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(d); // en-CA -> "YYYY-MM-DD"
}

function toIsoDateUTC(d) {
  return d.toISOString().slice(0, 10);
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function normalizeName(name) {
  const t = String(name || "")
    .trim()
    .replace(/\s+/g, " ");

  if (!t) return "";

  // "Nachname, Vorname" -> "Vorname Nachname"
  if (t.includes(",")) {
    const parts = t.split(",").map(s => s.trim()).filter(Boolean);
    if (parts.length >= 2) {
      const last = parts[0];
      const first = parts.slice(1).join(" ");
      return `${first} ${last}`.trim().toLowerCase();
    }
  }

  return t.toLowerCase();
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

  // DD.MM. (ohne Jahr) -> heuristisch Jahr
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
// MIGRATE (stabil: keine DROP staffplan!)
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

  // staffplan bleibt erhalten (nur beim Import leeren)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
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

  // Spalte requester_name nachrüsten falls alte Tabelle existierte
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS requester_name TEXT;`);

  // time_entries: wir lassen bestehende Tabelle, rüsten nur Felder nach
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
      break_minutes INT DEFAULT 0,
      auto_break_minutes INT DEFAULT 0,
      total_hours NUMERIC,
      overtime_hours NUMERIC,
      activity TEXT
    );
  `);

  // Breaks (für Pause Start/Ende)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS breaks (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ
    );
  `);

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
// HEALTH / BUILD
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));
app.get("/api/debug/build", (req, res) =>
  res.json({
    ok: true,
    build: "server.js FULL FINAL STABLE 2025-12-27",
    node: process.version,
    now: new Date().toISOString(),
  })
);

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
// EMPLOYEE TODAY (muss VOR /:id stehen!)
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const date = String(req.query.date || "").trim() || berlinISODate();

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
      [employeeId, date]
    );

    return res.json({ ok: true, date, projects: rows });
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
// STAFFPLAN IMPORT (robust + hybrid IDs)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.json({ ok: false, error: "Kein Worksheet gefunden" });

    // Excel-Mapping:
    // - customer: Spalte A (0)
    // - internal_po: Spalte B (1)
    // - customer_po: Spalte E (4)
    // - requester_name: Spalte I (8)  <-- ggf. anpassen
    // - employee_name: Spalte K (10)  <-- wie von dir gesagt
    const COL_CUSTOMER = 0;
    const COL_INTERNAL_PO = 1;
    const COL_CUSTOMER_PO = 4;
    const COL_REQUESTER = 8;
    const COL_EMPLOYEE_NAME = 10;

    // Date columns Bereich (wie bisher grob ab L bis ...):
    const startCol = 11; // L
    const endCol = 299;

    // --- 1) HeaderRow finden: Zeile mit den meisten parsebaren Datumszellen ---
    let headerRow = null;
    let bestCnt = 0;

    for (let r = 0; r <= 25; r++) {
      let cnt = 0;
      for (let c = startCol; c <= endCol; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        if (parseExcelDate(cell)) cnt++;
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = r;
      }
    }

    if (headerRow === null || bestCnt < 3) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..26)" });
    }

    // --- 2) Dates lückenlos pro Spalte bauen (Formeln ok) ---
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
        iso: toIsoDateUTC(d),
        cw: "CW" + getISOWeek(d),
      });
    }

    console.log(
      "📅 staffplan headerRow:", headerRow + 1,
      "first:", dates[0]?.iso,
      "last:", dates[dates.length - 1]?.iso,
      "cols:", dates.length
    );

    // --- 3) Mitarbeiter-Map laden (Hybrid) ---
    const empRes = await pool.query(`SELECT employee_id, name FROM employees`);
    const nameToId = new Map();
    for (const e of empRes.rows) {
      nameToId.set(normalizeName(e.name), e.employee_id);
    }

    // --- 4) staffplan leeren (nur hier!) ---
    await pool.query("TRUNCATE staffplan RESTART IDENTITY");

    let imported = 0;

    // --- 5) Datenzeilen: wie bei dir üblich r ab 5, step 2 ---
    for (let r = 5; r < 20000; r += 2) {
      const employeeCell = ws[XLSX.utils.encode_cell({ r, c: COL_EMPLOYEE_NAME })];
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: COL_REQUESTER })];

      if (!employeeCell?.v) continue;

      const employeeNameRaw = String(employeeCell.v).trim();
      const employeeNameNorm = normalizeName(employeeNameRaw);

      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      // employee_id bestimmen: vorhandene manuelle ID nutzen, sonst AUTO erzeugen
      let employeeId = nameToId.get(employeeNameNorm);

      if (!employeeId) {
        employeeId = `AUTO_${Date.now()}_${r}_${Math.floor(Math.random() * 1e6)}`;

        // UPSERT: keine PK-Kollisionen, keine Überschreibung von Mail/Sprache
        await pool.query(
          `
          INSERT INTO employees (employee_id, name)
          VALUES ($1, $2)
          ON CONFLICT (employee_id) DO UPDATE SET name = EXCLUDED.name
          `,
          [employeeId, employeeNameNorm.includes(",") ? employeeNameNorm : employeeNameRaw]
        );

        // Map updaten (wichtig!)
        nameToId.set(employeeNameNorm, employeeId);
      }

      const customer = ws[XLSX.utils.encode_cell({ r, c: COL_CUSTOMER })]?.v ?? null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: COL_INTERNAL_PO })]?.v ?? null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: COL_CUSTOMER_PO })]?.v ?? null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v ?? null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = (typeof planRaw === "number" && isFinite(planRaw)) ? planRaw : null;

        if (!proj && plan === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10)
          `,
          [
            employeeId,
            // speichere in staffplan „Vorname Nachname“ (auch wenn Excel "Nachname, Vorname" liefert)
            employeeNameNorm ? employeeNameNorm.split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ") : employeeNameRaw,
            requesterName,
            d.iso,
            d.cw,
            customer,
            internalPo,
            customerPo,
            proj,
            plan
          ]
        );

        imported++;
      }
    }

    return res.json({
      ok: true,
      imported,
      header_row: headerRow + 1,
      date_from: dates[0]?.iso,
      date_to: dates[dates.length - 1]?.iso,
      date_cols: dates.length,
    });
  } catch (e) {
    console.error("STAFFPLAN IMPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME TRACKING
// ======================================================
async function pickFirstTodayProject(employeeId, workDate) {
  const r = await pool.query(
    `
    SELECT customer_po, internal_po, project_short, requester_name
    FROM staffplan
    WHERE employee_id = $1 AND work_date = $2::date
    ORDER BY customer_po, internal_po
    LIMIT 1
    `,
    [employeeId, workDate]
  );
  return r.rowCount ? r.rows[0] : null;
}

app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    const r = await pool.query(
      `
      SELECT start_time
      FROM time_entries
      WHERE employee_id = $1 AND end_time IS NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );
    if (!r.rowCount) return res.json({ ok: false });
    return res.json({ ok: true, start_time: r.rows[0].start_time });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const workDate = berlinISODate();
    const now = new Date();

    // wenn schon offen -> zurückgeben
    const open = await pool.query(
      `
      SELECT id, start_time, customer_po, internal_po, project_short, requester_name
      FROM time_entries
      WHERE employee_id = $1 AND end_time IS NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );
    if (open.rowCount) {
      return res.json({
        ok: true,
        start_time: open.rows[0].start_time,
        picked_project: {
          customer_po: open.rows[0].customer_po,
          internal_po: open.rows[0].internal_po,
          project_short: open.rows[0].project_short,
          requester_name: open.rows[0].requester_name,
        },
        note: "already_running",
      });
    }

    // AUTO: erstes heutiges Projekt ziehen, wenn nichts ausgewählt
    const picked = await pickFirstTodayProject(employeeId, workDate);

    await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, customer_po, internal_po, project_short, requester_name, start_time, start_ts, activity)
      VALUES ($1,$2::date,$3,$4,$5,$6,$7,$8,$9)
      `,
      [
        employeeId,
        workDate,
        picked?.customer_po || null,
        picked?.internal_po || null,
        picked?.project_short || null,
        picked?.requester_name || null,
        now.toISOString(),
        now.toISOString(),
        String(req.body.activity || "Arbeitszeit"),
      ]
    );

    return res.json({
      ok: true,
      start_time: now.toISOString(),
      picked_project: picked,
    });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const activity = String(req.body.activity || "Arbeitszeit");

    const open = await pool.query(
      `
      SELECT *
      FROM time_entries
      WHERE employee_id = $1 AND end_time IS NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Kein offener Arbeitsblock" });

    const entry = open.rows[0];
    const workDate = berlinISODate(new Date(entry.work_date));

    // offene Pausen schließen (bis jetzt)
    await pool.query(
      `
      UPDATE breaks
      SET end_ts = $1
      WHERE employee_id = $2 AND work_date = $3::date AND end_ts IS NULL
      `,
      [now.toISOString(), employeeId, workDate]
    );

    // Pausenminuten summieren
    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)) / 60), 0)::int AS mins
      FROM breaks
      WHERE employee_id = $1 AND work_date = $2::date AND end_ts IS NOT NULL
      `,
      [employeeId, workDate]
    );
    const breakMinutes = br.rows[0].mins || 0;

    // Backfill Projekt, falls null
    let customer_po = entry.customer_po;
    let internal_po = entry.internal_po;
    let project_short = entry.project_short;
    let requester_name = entry.requester_name;

    if (!customer_po && !internal_po && !project_short) {
      const picked = await pickFirstTodayProject(employeeId, workDate);
      if (picked) {
        customer_po = picked.customer_po || null;
        internal_po = picked.internal_po || null;
        project_short = picked.project_short || null;
        requester_name = picked.requester_name || null;
      }
    }

    const start = new Date(entry.start_time || entry.start_ts);
    const diffMinutes = Math.max(0, Math.round((now.getTime() - start.getTime()) / 60000));
    const netMinutes = Math.max(0, diffMinutes - breakMinutes);
    const netHours = (netMinutes / 60).toFixed(2);

    await pool.query(
      `
      UPDATE time_entries
      SET end_time = $1,
          end_ts = $2,
          activity = $3,
          break_minutes = $4,
          customer_po = $5,
          internal_po = $6,
          project_short = $7,
          requester_name = $8,
          total_hours = $9::numeric
      WHERE id = $10
      `,
      [
        now.toISOString(),
        now.toISOString(),
        activity,
        breakMinutes,
        customer_po,
        internal_po,
        project_short,
        requester_name,
        netHours,
        entry.id,
      ]
    );

    return res.json({ ok: true, net_hours: netHours, break_minutes: breakMinutes });
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

    const workDate = berlinISODate();
    const now = new Date();

    // keine zweite offene Pause
    const open = await pool.query(
      `SELECT id FROM breaks WHERE employee_id=$1 AND work_date=$2::date AND end_ts IS NULL LIMIT 1`,
      [employeeId, workDate]
    );
    if (open.rowCount) return res.json({ ok: true, note: "already_running" });

    await pool.query(
      `INSERT INTO breaks (employee_id, work_date, start_ts) VALUES ($1,$2::date,$3)`,
      [employeeId, workDate, now.toISOString()]
    );

    return res.json({ ok: true });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const workDate = berlinISODate();
    const now = new Date();

    const open = await pool.query(
      `SELECT id, start_ts FROM breaks WHERE employee_id=$1 AND work_date=$2::date AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId, workDate]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Keine offene Pause" });

    const start = new Date(open.rows[0].start_ts);
    const mins = Math.max(0, Math.round((now.getTime() - start.getTime()) / 60000));

    await pool.query(`UPDATE breaks SET end_ts=$1 WHERE id=$2`, [now.toISOString(), open.rows[0].id]);

    return res.json({ ok: true, minutes: mins });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG ENDPOINTS (staffplan)
// ======================================================
app.get("/api/debug/staffplan-check", async (req, res) => {
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
          AND lower(trim(employee_name)) = lower(trim($2))
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
});

app.get("/api/debug/staffplan-minmax", async (req, res) => {
  const r = await pool.query(`
    SELECT
      MIN(work_date) AS min_date,
      MAX(work_date) AS max_date,
      COUNT(*)::int AS total
    FROM staffplan
  `);
  res.json({ ok: true, ...r.rows[0] });
});

app.get("/api/debug/staffplan-topdates", async (req, res) => {
  const r = await pool.query(`
    SELECT work_date, COUNT(*)::int AS cnt
    FROM staffplan
    GROUP BY work_date
    ORDER BY work_date DESC
    LIMIT 15
  `);
  res.json({ ok: true, rows: r.rows });
});

app.get("/api/debug/staffplan-rows", async (req, res) => {
  const date = String(req.query.date || "").trim();
  const employeeId = String(req.query.employee_id || "").trim();
  if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

  const r = await pool.query(
    `
    SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
    FROM staffplan
    WHERE work_date = $1::date
      AND ($2 = '' OR employee_id = $2)
    ORDER BY employee_id, customer_po, internal_po
    LIMIT 200
    `,
    [date, employeeId]
  );

  res.json({ ok: true, date, rows: r.rows });
});

// ======================================================
// DEBUG ENDPOINTS (time)
// ======================================================
app.get("/api/debug/time-rows", async (req, res) => {
  const r = await pool.query(`
    SELECT id, work_date, customer_po, internal_po, project_short, requester_name, start_time, end_time, activity
    FROM time_entries
    ORDER BY id DESC
    LIMIT 200
  `);
  res.json({ ok: true, rows: r.rows });
});

app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const delNull = await pool.query(
      `DELETE FROM time_entries WHERE employee_id=$1 AND start_time IS NULL RETURNING id`,
      [employeeId]
    );

    const closeOpen = await pool.query(
      `
      UPDATE time_entries
      SET end_time = start_time, end_ts = start_ts
      WHERE employee_id=$1 AND end_time IS NULL AND start_time IS NOT NULL
      RETURNING id
      `,
      [employeeId]
    );

    const closeBreaks = await pool.query(
      `
      UPDATE breaks
      SET end_ts = start_ts
      WHERE employee_id=$1 AND end_ts IS NULL
      RETURNING id
      `,
      [employeeId]
    );

    res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: delNull.rowCount,
      closed_open_time_entries: closeOpen.rowCount,
      closed_open_breaks: closeBreaks.rowCount,
      deleted_closed_null_project: 0,
    });
  } catch (e) {
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
