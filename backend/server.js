console.log("🔥🔥🔥 SERVER.JS VERSION 2025-12-27 (FULL FINAL + BREAK_ENTRIES SELF-HEAL) 🔥🔥🔥");

/**
 * backend/server.js
 *
 * - /api/employee/today vor /api/employee/:id (Routing-Fix)
 * - Staffplan Import: requester in Spalte I (c=8), Mitarbeiter in Spalte K (c=10), Datums-Spalten ab L (c=11)
 * - Time Tracking stabil über start_time/end_time (und optional start_ts/end_ts)
 * - break_entries wird robust “self-healed” (end_ts existiert garantiert)
 */

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit"); // reserved for later
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

// Excel date parsing (xlsx berechnet Formeln NICHT)
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

  // DD.MM. irgendwo im Text -> heuristisch Jahr
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

function swapCommaName(name) {
  const s = String(name || "").trim();
  if (!s.includes(",")) return s;
  const parts = s.split(",");
  const last = (parts[0] || "").trim();
  const first = (parts.slice(1).join(",") || "").trim();
  return (`${first} ${last}`).trim() || s;
}

function normName(name) {
  return String(name || "")
    .trim()
    .replace(/,/g, "")
    .replace(/\s+/g, " ")
    .toLowerCase();
}

// ======================================================
// DB SELF-HEAL
// ======================================================
async function ensureEmployeesSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);
}

async function ensureStaffplanSchemaFresh() {
  await pool.query(`DROP TABLE IF EXISTS public.staffplan CASCADE`);
  await pool.query(`
    CREATE TABLE public.staffplan (
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
}

async function ensureTimeEntriesSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.time_entries (
      id BIGSERIAL PRIMARY KEY
    );
  `);

  const cols = [
    ["employee_id", "TEXT"],
    ["work_date", "DATE"],

    ["start_time", "TIMESTAMP"],
    ["end_time", "TIMESTAMP"],

    ["break_minutes", "INTEGER"],
    ["auto_break_minutes", "INTEGER"],
    ["total_hours", "NUMERIC"],
    ["overtime_hours", "NUMERIC"],

    ["activity", "TEXT"],
    ["internal_po", "TEXT"],
    ["project_short", "TEXT"],
    ["requester_name", "TEXT"],
    ["customer_po", "TEXT"],

    ["start_ts", "TIMESTAMPTZ"],
    ["end_ts", "TIMESTAMPTZ"],
  ];

  for (const [col, typ] of cols) {
    await pool.query(`ALTER TABLE public.time_entries ADD COLUMN IF NOT EXISTS ${col} ${typ};`);
  }
}

// ✅ FIX: break_entries robust (end_ts wird garantiert nachgezogen)
async function ensureBreakEntriesSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.break_entries (
      id BIGSERIAL PRIMARY KEY
    );
  `);

  await pool.query(`ALTER TABLE public.break_entries ADD COLUMN IF NOT EXISTS employee_id TEXT;`);
  await pool.query(`ALTER TABLE public.break_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE public.break_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ;`);
}

// ======================================================
// MIGRATE
// ======================================================
async function migrate() {
  console.log("🔧 DB migrate start");
  await ensureEmployeesSchema();
  await ensureStaffplanSchemaFresh(); // staffplan always fresh
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
// HEALTH + BUILD
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/api/debug/build", (req, res) => {
  res.json({
    ok: true,
    build: "server.js FULL FINAL + BUILD DEBUG 2025-12-27",
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
  const r = await pool.query(`SELECT * FROM public.employees ORDER BY name`);
  res.json(r.rows);
});

// ======================================================
// EMPLOYEE – TODAY (MUST be before /:id)
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const dateOverride = String(req.query.date || "").trim();
    const today = dateOverride || new Date().toISOString().slice(0, 10);

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
      FROM public.staffplan
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
// EMPLOYEE – SINGLE
// ======================================================
app.get("/api/employee/:id", async (req, res) => {
  const r = await pool.query(
    `SELECT employee_id,name,email,language FROM public.employees WHERE employee_id=$1`,
    [req.params.id]
  );
  if (!r.rowCount) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});

// ======================================================
// STAFFPLAN IMPORT
// requester_name = I (c=8)  -> Ansprechpartner (z.B. Hoffmann)
// employee_name  = K (c=10) -> "Irrgang, Jens"
// date columns start at L (c=11)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.status(400).json({ ok: false, error: "Kein Worksheet gefunden" });

    const ref = ws["!ref"] || "A1:A1";
    const range = XLSX.utils.decode_range(ref);

    const startCol = 11; // L
    const endCol = range.e.c;

    // 1) header row scan (0..20)
    let headerRow = null;
    let bestCnt = 0;
    for (let r = 0; r <= Math.min(range.e.r, 20); r++) {
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
    if (headerRow === null || bestCnt < 1) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..21)" });
    }

    // 2) first real date
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
    if (!baseDate) return res.json({ ok: false, error: "Header gefunden, aber kein erstes Datum parsebar" });

    // 3) build dates lückenlos über alle Spalten
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

      dates.push({ col: c, iso: toIsoDate(d), cw: "CW" + getISOWeek(d) });
    }

    console.log(
      "📅 HeaderRow:", headerRow + 1,
      "First:", dates[0]?.iso,
      "Last:", dates[dates.length - 1]?.iso,
      "count:", dates.length
    );

    await pool.query("DELETE FROM public.staffplan");
    let imported = 0;

    for (let r = 5; r < 20000; r += 2) {
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })]; // I
      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      const employeeCell = ws[XLSX.utils.encode_cell({ r, c: 10 })]; // K
      const employeeNameRaw = employeeCell?.v ? String(employeeCell.v).trim() : null;
      if (!employeeNameRaw) continue;

      const employeeNameSwapped = swapCommaName(employeeNameRaw);
      const candidates = Array.from(
        new Set([normName(employeeNameRaw), normName(employeeNameSwapped)])
      ).filter(Boolean);

      const emp = await pool.query(
        `
        SELECT employee_id, name
        FROM public.employees
        WHERE lower(regexp_replace(regexp_replace(trim(name), ',', '', 'g'), '\\s+', ' ', 'g')) = ANY($1)
        LIMIT 1
        `,
        [candidates]
      );

      let employeeId;
      let employeeName;

      if (emp.rowCount > 0) {
        employeeId = emp.rows[0].employee_id;
        employeeName = emp.rows[0].name;
      } else {
        employeeName = employeeNameSwapped || employeeNameRaw;
        employeeId = "AUTO" + r;
        await pool.query(`INSERT INTO public.employees (employee_id,name) VALUES ($1,$2)`, [
          employeeId,
          employeeName,
        ]);
      }

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const planned = typeof planRaw === "number" && isFinite(planRaw) ? planRaw : null;

        if (!proj && planned === null) continue;

        await pool.query(
          `
          INSERT INTO public.staffplan
            (employee_id,employee_name,requester_name,work_date,calendar_week,
             customer,internal_po,customer_po,project_short,planned_hours)
          VALUES ($1,$2,$3,$4::date,$5,$6,$7,$8,$9,$10)
          `,
          [employeeId, employeeName, requesterName, d.iso, d.cw, customer, internalPo, customerPo, proj, planned]
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
// BREAKS
// ======================================================
app.post("/api/break/start", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id FROM public.break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true });

    await pool.query(`INSERT INTO public.break_entries (employee_id, start_ts) VALUES ($1, $2)`, [
      employeeId,
      new Date(),
    ]);

    return res.json({ ok: true });
  } catch (e) {
    console.error("BREAK START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/end", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id, start_ts FROM public.break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Keine laufende Pause" });

    const endTs = new Date();
    const startTs = new Date(open.rows[0].start_ts);

    await pool.query(`UPDATE public.break_entries SET end_ts=$1 WHERE id=$2`, [endTs, open.rows[0].id]);

    const minutes = Math.max(0, Math.round((endTs.getTime() - startTs.getTime()) / 60000));
    return res.json({ ok: true, minutes });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: current/start/end (ONLY TODAY)
// Source of truth: start_time/end_time
// ======================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.params.employee_id || "").trim();
    const today = toIsoDate(new Date());

    const r = await pool.query(
      `
      SELECT id, start_time
      FROM public.time_entries
      WHERE employee_id=$1
        AND work_date = $2::date
        AND start_time IS NOT NULL
        AND end_time IS NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId, today]
    );

    if (!r.rowCount) return res.json({ ok: false });
    return res.json({ ok: true, start_time: r.rows[0].start_time });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/start", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const now = new Date();
    const today = toIsoDate(now);

    const open = await pool.query(
      `
      SELECT id, start_time
      FROM public.time_entries
      WHERE employee_id=$1
        AND work_date = $2::date
        AND start_time IS NOT NULL
        AND end_time IS NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId, today]
    );
    if (open.rowCount) return res.json({ ok: true, start_time: open.rows[0].start_time });

    const ins = await pool.query(
      `
      INSERT INTO public.time_entries
        (employee_id, work_date, activity, start_time)
      VALUES ($1, $2::date, $3, $4)
      RETURNING id, start_time
      `,
      [employeeId, today, "Arbeitszeit", now]
    );

    // optional best-effort start_ts
    try {
      await pool.query(`UPDATE public.time_entries SET start_ts=$1 WHERE id=$2`, [now, ins.rows[0].id]);
    } catch (e) {
      console.warn("start_ts update skipped:", e.message);
    }

    return res.json({ ok: true, start_time: ins.rows[0].start_time });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/end", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const activity = req.body.activity ? String(req.body.activity).trim() : "Arbeitszeit";

    const now = new Date();
    const today = toIsoDate(now);

    const open = await pool.query(
      `
      SELECT id, start_time
      FROM public.time_entries
      WHERE employee_id=$1
        AND work_date = $2::date
        AND start_time IS NOT NULL
        AND end_time IS NULL
      ORDER BY start_time DESC
      LIMIT 1
      `,
      [employeeId, today]
    );

    if (!open.rowCount) {
      return res.status(400).json({ ok: false, error: "Kein laufender Arbeitsblock (heute)" });
    }

    const id = open.rows[0].id;
    const startTime = new Date(open.rows[0].start_time);

    await pool.query(
      `UPDATE public.break_entries SET end_ts=$1 WHERE employee_id=$2 AND end_ts IS NULL`,
      [now, employeeId]
    );

    await pool.query(
      `
      UPDATE public.time_entries
      SET end_time = $1,
          activity = $2
      WHERE id = $3
      `,
      [now, activity, id]
    );

    // optional best-effort end_ts
    try {
      await pool.query(`UPDATE public.time_entries SET end_ts=$1 WHERE id=$2`, [now, id]);
    } catch (e) {
      console.warn("end_ts update skipped:", e.message);
    }

    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000), 0)::bigint AS ms
      FROM public.break_entries
      WHERE employee_id = $1
        AND end_ts IS NOT NULL
        AND start_ts >= $2::timestamptz
        AND end_ts <= $3::timestamptz
      `,
      [employeeId, startTime, now]
    );

    const breakMs = Number(br.rows[0]?.ms || 0);
    const totalMs = Math.max(0, now.getTime() - startTime.getTime());
    const netMs = Math.max(0, totalMs - breakMs);

    return res.json({
      ok: true,
      net_hours: (netMs / 3600000).toFixed(2),
      break_minutes: Math.round(breakMs / 60000),
    });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG ENDPOINTS
// ======================================================
app.get("/api/debug/time-entries", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT id, work_date, customer_po, internal_po, project_short, requester_name,
             start_time, end_time, activity
      FROM public.time_entries
      WHERE employee_id = $1
      ORDER BY start_time DESC NULLS LAST, id DESC
      LIMIT 50
      `,
      [employeeId]
    );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("DEBUG TIME ENTRIES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/time-entries-columns", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT column_name, data_type
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='time_entries'
      ORDER BY ordinal_position
    `);
    return res.json({ ok: true, columns: r.rows });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/staffplan-on-date", async (req, res) => {
  const date = String(req.query.date || "").trim();
  if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

  const r = await pool.query(
    `
    SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
    FROM public.staffplan
    WHERE work_date = $1::date
    ORDER BY employee_name, customer_po, internal_po
    LIMIT 200
    `,
    [date]
  );

  return res.json({ ok: true, date, rows: r.rows });
});

// ======================================================
// DEBUG: cleanup-time (einmalig zum Aufräumen)
// - löscht kaputte Rows ohne start_time
// - schließt alle offenen Blocks (end_time NULL) auf start_time (0h)
// - schließt offene Pausen (end_ts NULL) auf start_ts
// ======================================================
app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const delNullStart = await pool.query(
      `DELETE FROM public.time_entries WHERE employee_id=$1 AND start_time IS NULL`,
      [employeeId]
    );

    const closeOpen = await pool.query(
      `
      UPDATE public.time_entries
      SET end_time = start_time
      WHERE employee_id=$1
        AND start_time IS NOT NULL
        AND end_time IS NULL
      `,
      [employeeId]
    );

    const closeBreaks = await pool.query(
      `UPDATE public.break_entries SET end_ts = start_ts WHERE employee_id=$1 AND end_ts IS NULL`,
      [employeeId]
    );

    return res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: delNullStart.rowCount,
      closed_open_time_entries: closeOpen.rowCount,
      closed_open_breaks: closeBreaks.rowCount,
    });
  } catch (e) {
    console.error("CLEANUP ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
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
