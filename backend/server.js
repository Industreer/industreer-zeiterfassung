console.log("🔥🔥🔥 SERVER.JS FULL FINAL + AUTO FIRST TODAY PROJECT + STAFFPLAN HEADER TODAY-PRIO 2025-12-27 🔥🔥🔥");
/**
 * backend/server.js
 *
 * Features (stable):
 * - Express + Postgres (Render)
 * - Logo upload
 * - Employees: list + get + upsert (hybrid manuell/auto)
 * - Staffplan Import (robust): HeaderRow nach "heute" priorisieren + Dates lückenlos bauen
 * - /api/employee/today (steht VOR /api/employee/:id)
 * - Time tracking: /api/time/start, /api/time/end, /api/time/current/:id
 * - Breaks: /api/break/start, /api/break/end
 * - Debug Endpoints: build, staffplan-minmax/topdates/check/rows, time-rows, repair-time-schema, cleanup-time
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
app.use(express.json({ limit: "5mb" }));
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
function berlinISODate(d = new Date()) {
  // "YYYY-MM-DD" in Europe/Berlin
  const s = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(d);
  return s; // en-CA liefert YYYY-MM-DD
}

function toIsoDateUTC(d) {
  // d als Date -> YYYY-MM-DD (UTC)
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function getISOWeek(date) {
  // ISO week number (UTC)
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  const day = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - day);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function normalizeName(s) {
  return String(s || "")
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

/**
 * Excel-Date -> Date (UTC midnight-ish)
 * Unterstützt:
 * - Seriennummer
 * - "27.12.2025"
 * - "Sa 27.12.2025"
 * - "27.12." oder "Sa 27.12." (Jahr heuristisch nahe heute)
 */
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
// MIGRATE / REPAIR SCHEMA
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

  // staffplan immer frisch
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

  // time_entries + breaks persistent, aber wir "reparieren" falls Spalten fehlen
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      internal_po TEXT,
      project_short TEXT,
      requester_name TEXT,
      activity TEXT,
      start_ts TIMESTAMPTZ,
      end_ts TIMESTAMPTZ,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      break_minutes INTEGER DEFAULT 0
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS breaks (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ
    );
  `);

  // Repair columns (idempotent)
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS project_short TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS requester_name TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS activity TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_time TIMESTAMP;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_time TIMESTAMP;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS break_minutes INTEGER DEFAULT 0;`);

  console.log("✅ DB migrate finished");
}

async function listTimeColumns() {
  const r = await pool.query(
    `
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema='public' AND table_name='time_entries'
    ORDER BY ordinal_position
    `
  );
  return r.rows;
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

app.get("/api/debug/build", (req, res) => {
  res.json({
    ok: true,
    build: "server.js FULL FINAL + STAFFPLAN TODAY-PRIO + AUTO FIRST TODAY PROJECT 2025-12-27",
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
  const r = await pool.query(`SELECT employee_id,name,email,language FROM employees ORDER BY name`);
  res.json(r.rows);
});

// Hybrid Admin: employee upsert (manuell oder update)
app.post("/api/employees/upsert", async (req, res) => {
  try {
    const employee_id = String(req.body.employee_id || "").trim();
    const name = String(req.body.name || "").trim();
    const email = req.body.email ? String(req.body.email).trim() : null;
    const language = req.body.language ? String(req.body.language).trim() : "de";

    if (!employee_id || !name) {
      return res.status(400).json({ ok: false, error: "employee_id und name sind Pflicht" });
    }

    await pool.query(
      `
      INSERT INTO employees (employee_id, name, email, language)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (employee_id)
      DO UPDATE SET name=EXCLUDED.name, email=EXCLUDED.email, language=EXCLUDED.language
      `,
      [employee_id, name, email, language]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("EMPLOYEE UPSERT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
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
    const today = dateOverride || berlinISODate();

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
      ORDER BY customer_po, internal_po, id
      `,
      [employeeId, today]
    );

    return res.json({
      ok: true,
      date: today,
      projects: rows,
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
// STAFFPLAN IMPORT (robust: HeaderRow heute-prio + Dates lückenlos)
// IMPORTANT: In eurer Excel:
// - requester_name steht bei euch in Spalte I (Index 8)
// - employee_name steht bei euch in Spalte K (Index 10) z.B. "Irrgang, Jens"
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws || !ws["!ref"]) return res.json({ ok: false, error: "Leeres Sheet / !ref fehlt" });

    const range = XLSX.utils.decode_range(ws["!ref"]);

    // Heuristik: Datumsbereich typischerweise ab Spalte L (Index 11), aber wir lassen etwas Luft.
    const startCol = Math.max(0, 10);
    const endCol = range.e.c;

    // --- 1) HeaderRow finden (heute priorisieren) ---
    const todayIso = berlinISODate(); // "YYYY-MM-DD"
    const todayDateUTC = new Date(todayIso + "T00:00:00.000Z");

    let headerRow = null;
    let bestScore = -1;

    for (let r = 0; r <= Math.min(200, range.e.r); r++) {
      let cnt = 0;
      let bestDistDays = Infinity;

      for (let c = startCol; c <= endCol; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const d = parseExcelDate(cell);
        if (!d) continue;

        cnt++;
        const distDays = Math.abs(Math.round((d.getTime() - todayDateUTC.getTime()) / 86400000));
        if (distDays < bestDistDays) bestDistDays = distDays;
      }

      if (cnt < 3) continue;

      const nearTodayBonus = bestDistDays <= 10 ? 1000 : 0;
      const score = cnt + nearTodayBonus - bestDistDays;

      if (score > bestScore) {
        bestScore = score;
        headerRow = r;
      }
    }

    if (headerRow === null) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..201)" });
    }

    // --- 2) Dates lückenlos pro Spalte bauen (Formeln ohne cached value abfangen) ---
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

    if (!dates.length) {
      return res.json({ ok: false, error: "Datumszeile gefunden, aber keine Datumsspalten erzeugt" });
    }

    // --- 2b) Validierung: heute muss im Range liegen, sonst Import abbrechen (schützt vor falschem Header) ---
    const firstIso = dates[0].iso;
    const lastIso = dates[dates.length - 1].iso;

    if (todayIso < firstIso || todayIso > lastIso) {
      return res.json({
        ok: false,
        error: `Import-Header falsch erkannt: Range=${firstIso}..${lastIso}, aber heute=${todayIso} liegt außerhalb.`,
        header_row: headerRow + 1,
        date_from: firstIso,
        date_to: lastIso,
        date_cols: dates.length,
      });
    }

    console.log(
      "📅 staffplan headerRow:", headerRow + 1,
      "first:", firstIso,
      "last:", lastIso,
      "cols:", dates.length,
      "today:", todayIso
    );

    // --- 3) staffplan leeren ---
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // --- 4) Zeilen durchgehen (wie bisher: r=5, Schritt 2) ---
    // Excel-Spalten:
    // - requester_name: I (c=8)  -> "Hoffmann"
    // - employee_name:  K (c=10) -> "Irrgang, Jens"
    // - customer:       A (c=0)
    // - internal_po:    B (c=1)
    // - customer_po:    E (c=4)
    for (let r = 5; r < Math.min(20000, range.e.r + 1); r += 2) {
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const employeeCell = ws[XLSX.utils.encode_cell({ r, c: 10 })];

      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;
      const employeeNameRaw = employeeCell?.v ? String(employeeCell.v).trim() : null;

      // Fallback, falls K leer ist:
      const employeeName = employeeNameRaw || (requesterCell?.v ? String(requesterCell.v).trim() : null);
      if (!employeeName) continue;

      // 4a) employee_id ermitteln:
      // - zuerst: exakten/normalisierten Namensmatch in employees
      // - sonst: AUTO+r
      const empFind = await pool.query(
        `
        SELECT employee_id
        FROM employees
        WHERE lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) =
              lower(regexp_replace(trim($1), '\\s+', ' ', 'g'))
        LIMIT 1
        `,
        [employeeName.includes(",") ? employeeName.replace(",", "") : employeeName]
      );

      let employeeId;
      if (empFind.rowCount) {
        employeeId = empFind.rows[0].employee_id;
      } else {
        employeeId = "AUTO" + r;
        // UPSERT: verhindert duplicate key (bei erneutem Import)
        await pool.query(
          `
          INSERT INTO employees (employee_id, name)
          VALUES ($1,$2)
          ON CONFLICT (employee_id)
          DO UPDATE SET name=EXCLUDED.name
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
        const plan =
          typeof planRaw === "number" && isFinite(planRaw)
            ? planRaw
            : null;

        if (!proj && plan === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          `,
          [
            employeeId,
            employeeName,
            requesterName,
            d.iso,
            d.cw,
            customer,
            internalPo,
            customerPo,
            proj,
            plan,
          ]
        );

        imported++;
      }
    }

    return res.json({
      ok: true,
      imported,
      header_row: headerRow + 1,
      date_from: firstIso,
      date_to: lastIso,
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
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT start_ts, start_time
      FROM time_entries
      WHERE employee_id=$1 AND end_ts IS NULL
      ORDER BY COALESCE(start_ts, start_time) DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!r.rowCount) return res.json({ ok: false });
    const row = r.rows[0];
    const start = row.start_ts || row.start_time;
    return res.json({ ok: true, start_time: start });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const workDate = berlinISODate(now);

    // Wenn schon offen -> return existing
    const open = await pool.query(
      `
      SELECT id, start_ts, start_time, customer_po, internal_po, project_short, requester_name
      FROM time_entries
      WHERE employee_id=$1 AND end_ts IS NULL
      ORDER BY COALESCE(start_ts, start_time) DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (open.rowCount) {
      const o = open.rows[0];
      return res.json({
        ok: true,
        start_time: o.start_ts || o.start_time,
        already_running: true,
        picked_project: {
          customer_po: o.customer_po || null,
          internal_po: o.internal_po || null,
          project_short: o.project_short || null,
          requester_name: o.requester_name || null,
        },
      });
    }

    // Project aus Body oder automatisch erstes heutiges staffplan-Projekt
    let pickedProject = null;

    const bodyProject = {
      customer_po: req.body.customer_po ? String(req.body.customer_po).trim() : null,
      internal_po: req.body.internal_po ? String(req.body.internal_po).trim() : null,
      project_short: req.body.project_short ? String(req.body.project_short).trim() : null,
      requester_name: req.body.requester_name ? String(req.body.requester_name).trim() : null,
    };

    if (bodyProject.customer_po || bodyProject.internal_po || bodyProject.project_short) {
      pickedProject = bodyProject;
    } else {
      const sp = await pool.query(
        `
        SELECT customer_po, internal_po, project_short, requester_name
        FROM staffplan
        WHERE employee_id=$1 AND work_date=$2::date
        ORDER BY customer_po, internal_po, id
        LIMIT 1
        `,
        [employeeId, workDate]
      );
      if (sp.rowCount) {
        pickedProject = sp.rows[0];
      }
    }

    const activity = req.body.activity ? String(req.body.activity).trim() : "Arbeitszeit";

    await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, customer_po, internal_po, project_short, requester_name,
         activity, start_ts, start_time)
      VALUES ($1,$2::date,$3,$4,$5,$6,$7,$8,$9)
      `,
      [
        employeeId,
        workDate,
        pickedProject?.customer_po || null,
        pickedProject?.internal_po || null,
        pickedProject?.project_short || null,
        pickedProject?.requester_name || null,
        activity,
        now,
        now,
      ]
    );

    res.json({
      ok: true,
      start_time: now.toISOString(),
      picked_project: pickedProject,
    });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();

    // offene breaks schließen
    await pool.query(
      `
      UPDATE breaks
      SET end_ts=$2
      WHERE employee_id=$1 AND end_ts IS NULL
      `,
      [employeeId, now]
    );

    const open = await pool.query(
      `
      SELECT id, start_ts, start_time
      FROM time_entries
      WHERE employee_id=$1 AND end_ts IS NULL
      ORDER BY COALESCE(start_ts, start_time) DESC
      LIMIT 1
      `,
      [employeeId]
    );

    if (!open.rowCount) {
      return res.json({ ok: false, error: "Kein laufender Arbeitsblock gefunden" });
    }

    const row = open.rows[0];
    const start = row.start_ts || row.start_time;
    if (!start) return res.json({ ok: false, error: "Startzeit fehlt im Datensatz" });

    // Break-Minuten zwischen Start und Ende berechnen
    const br = await pool.query(
      `
      SELECT
        COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)))/60, 0)::int AS mins
      FROM breaks
      WHERE employee_id=$1
        AND end_ts IS NOT NULL
        AND start_ts >= $2
        AND end_ts <= $3
      `,
      [employeeId, start, now]
    );

    const breakMinutes = br.rows[0]?.mins || 0;

    // Activity update (optional)
    const activity = req.body.activity ? String(req.body.activity).trim() : null;

    await pool.query(
      `
      UPDATE time_entries
      SET end_ts=$2,
          end_time=$2,
          break_minutes=$3,
          activity = COALESCE($4, activity)
      WHERE id=$1
      `,
      [row.id, now, breakMinutes, activity]
    );

    const diffSeconds = Math.max(0, Math.round((now.getTime() - new Date(start).getTime()) / 1000));
    const netSeconds = Math.max(0, diffSeconds - breakMinutes * 60);
    const netHours = (netSeconds / 3600).toFixed(2);

    res.json({
      ok: true,
      net_hours: netHours,
      break_minutes: breakMinutes,
    });
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
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();

    // Wenn schon offene Pause existiert -> ok
    const open = await pool.query(
      `SELECT id, start_ts FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );

    if (open.rowCount) {
      return res.json({ ok: true, already_running: true, start_time: open.rows[0].start_ts });
    }

    await pool.query(
      `INSERT INTO breaks (employee_id, start_ts) VALUES ($1,$2)`,
      [employeeId, now]
    );

    res.json({ ok: true, start_time: now.toISOString() });
  } catch (e) {
    console.error("BREAK START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();

    const open = await pool.query(
      `SELECT id, start_ts FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );

    if (!open.rowCount) {
      return res.json({ ok: false, error: "Keine offene Pause gefunden" });
    }

    const start = open.rows[0].start_ts;
    await pool.query(`UPDATE breaks SET end_ts=$2 WHERE id=$1`, [open.rows[0].id, now]);

    const minutes = Math.max(0, Math.round((now.getTime() - new Date(start).getTime()) / 60000));
    res.json({ ok: true, minutes });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: STAFFPLAN
// ======================================================
app.get("/api/debug/staffplan-minmax", async (req, res) => {
  const r = await pool.query(
    `SELECT MIN(work_date) AS min_date, MAX(work_date) AS max_date, COUNT(*)::int AS total FROM staffplan`
  );
  res.json({ ok: true, ...r.rows[0] });
});

app.get("/api/debug/staffplan-topdates", async (req, res) => {
  const r = await pool.query(
    `
    SELECT work_date, COUNT(*)::int AS cnt
    FROM staffplan
    GROUP BY work_date
    ORDER BY work_date DESC
    LIMIT 30
    `
  );
  res.json({ ok: true, rows: r.rows });
});

// detail rows for a date + optional employee_id
app.get("/api/debug/staffplan-rows", async (req, res) => {
  try {
    const date = String(req.query.date || berlinISODate()).trim();
    const employeeId = req.query.employee_id ? String(req.query.employee_id).trim() : null;

    const r = employeeId
      ? await pool.query(
          `
          SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
          FROM staffplan
          WHERE work_date=$1::date AND employee_id=$2
          ORDER BY customer_po, internal_po, id
          `,
          [date, employeeId]
        )
      : await pool.query(
          `
          SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
          FROM staffplan
          WHERE work_date=$1::date
          ORDER BY employee_id, customer_po, internal_po, id
          LIMIT 200
          `,
          [date]
        );

    res.json({ ok: true, date, rows: r.rows });
  } catch (e) {
    console.error("STAFFPLAN ROWS DEBUG ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// staffplan-check (für eure alte Debug-Ausgabe)
app.get("/api/debug/staffplan-check", async (req, res) => {
  const employeeId = req.query.employee_id ? String(req.query.employee_id).trim() : null;
  const date = String(req.query.date || "").trim();

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
    staffplan_for_employee_name: byName ? byName.rows[0].cnt : null,
  });
});

// ======================================================
// DEBUG: TIME
// ======================================================
app.get("/api/debug/time-rows", async (req, res) => {
  try {
    const employeeId = req.query.employee_id ? String(req.query.employee_id).trim() : null;

    const r = employeeId
      ? await pool.query(
          `
          SELECT id, work_date, customer_po, internal_po, project_short, requester_name,
                 start_time, end_time, activity
          FROM time_entries
          WHERE employee_id=$1
          ORDER BY id DESC
          LIMIT 200
          `,
          [employeeId]
        )
      : await pool.query(
          `
          SELECT id, employee_id, work_date, customer_po, internal_po, project_short, requester_name,
                 start_time, end_time, activity
          FROM time_entries
          ORDER BY id DESC
          LIMIT 200
          `
        );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("TIME ROWS DEBUG ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Repariert time_entries schema (falls du jemals wieder "column ... does not exist" siehst)
app.get("/api/debug/repair-time-schema", async (req, res) => {
  try {
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_time TIMESTAMP;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_time TIMESTAMP;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS break_minutes INTEGER DEFAULT 0;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS project_short TEXT;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS requester_name TEXT;`);
    await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS activity TEXT;`);

    const cols = await listTimeColumns();
    res.json({ ok: true, repaired: true, columns: cols });
  } catch (e) {
    console.error("REPAIR TIME SCHEMA ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Cleanup: schließt offene Einträge / Pausen, löscht kaputte rows (start_time null etc.)
app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // 1) kaputte rows (start_time/start_ts null) löschen
    const del = await pool.query(
      `
      DELETE FROM time_entries
      WHERE employee_id=$1
        AND (start_ts IS NULL AND start_time IS NULL)
      `,
      [employeeId]
    );

    // 2) offene Arbeitsblöcke schließen (end = start, damit keine "hängen" bleiben)
    const closed = await pool.query(
      `
      UPDATE time_entries
      SET end_ts = COALESCE(end_ts, start_ts),
          end_time = COALESCE(end_time, start_time)
      WHERE employee_id=$1
        AND end_ts IS NULL
        AND (start_ts IS NOT NULL OR start_time IS NOT NULL)
      `,
      [employeeId]
    );

    // 3) offene breaks schließen
    const br = await pool.query(
      `
      UPDATE breaks
      SET end_ts = COALESCE(end_ts, start_ts)
      WHERE employee_id=$1 AND end_ts IS NULL
      `,
      [employeeId]
    );

    // 4) optionale "closed aber projekt null" NICHT löschen (dein Wunsch: Daten behalten)
    // Falls du es irgendwann willst, kannst du hier wieder löschen.

    res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: del.rowCount || 0,
      closed_open_time_entries: closed.rowCount || 0,
      closed_open_breaks: br.rowCount || 0,
      deleted_closed_null_project: 0,
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
