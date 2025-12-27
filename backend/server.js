console.log("🔥🔥🔥 SERVER.JS VERSION 2025-12-27 (SELF-HEAL time_entries schema) 🔥🔥🔥");

const path = require("path");
const fs = require("fs");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
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

// Excel date parsing (formulas are not evaluated by xlsx -> import builds dates lückenlos)
function parseExcelDate(cell) {
  if (!cell) return null;

  // Excel serial
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
// DB SELF-HEAL (WICHTIG!)
// ======================================================
async function ensureTimeEntriesSchema() {
  // Minimal table
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY
    );
  `);

  // Columns (safe upgrades)
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS employee_id TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS work_date DATE;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS project_short TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS requester_name TEXT;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS start_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS end_ts TIMESTAMPTZ;`);
  await pool.query(`ALTER TABLE time_entries ADD COLUMN IF NOT EXISTS activity TEXT;`);
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

  // staffplan ALWAYS fresh (per your design)
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

  // upgrade persistent tables
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
// EMPLOYEE – HEUTIGE PROJEKTE (WICHTIG: vor /:id)
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
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2
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
// STAFFPLAN IMPORT
// requester_name = Spalte I (index 8)
// employee_name  = Spalte K (index 10) (z.B. "Irrgang, Jens")
// date columns start at L (index 11)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    if (!ws) return res.status(400).json({ ok: false, error: "Kein Worksheet gefunden" });

    const ref = ws["!ref"] || "A1:A1";
    const range = XLSX.utils.decode_range(ref);

    const startCol = 11;      // ab Spalte L
    const endCol = range.e.c; // bis letzte benutzte Spalte

    // 1) header row: row with most parsable dates
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

    // 2) first real date in header row
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
      return res.json({ ok: false, error: "Header gefunden, aber kein erstes Datum parsebar" });
    }

    // 3) build dates for ALL columns (lückenlos, auch wenn Formeln keine cached Values liefern)
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

    // 4) clear staffplan
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // 5) scan employees rows (your format: start r=5, step 2)
    for (let r = 5; r < 20000; r += 2) {
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })]; // I
      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      const employeeCell = ws[XLSX.utils.encode_cell({ r, c: 10 })]; // K
      const employeeNameRaw = employeeCell?.v ? String(employeeCell.v).trim() : null;
      if (!employeeNameRaw) continue;

      const employeeNameSwapped = swapCommaName(employeeNameRaw);
      const candidates = Array.from(new Set([normName(employeeNameRaw), normName(employeeNameSwapped)])).filter(Boolean);

      const emp = await pool.query(
        `
        SELECT employee_id, name
        FROM employees
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
        await pool.query(
          `INSERT INTO employees (employee_id,name) VALUES ($1,$2)`,
          [employeeId, employeeName]
        );
      }

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const planned = (typeof planRaw === "number" && isFinite(planRaw)) ? planRaw : null;

        if (!proj && planned === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id,employee_name,requester_name,work_date,calendar_week,
             customer,internal_po,customer_po,project_short,planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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
// TIME: current running
// ======================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.params.employee_id || "").trim();
    const r = await pool.query(
      `SELECT start_ts FROM time_entries
       WHERE employee_id=$1 AND end_ts IS NULL AND start_ts IS NOT NULL
       ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (!r.rowCount) return res.json({ ok: false });
    return res.json({ ok: true, start_time: r.rows[0].start_ts });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: start
// ======================================================
app.post("/api/time/start", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const customerPo = req.body.customer_po ? String(req.body.customer_po).trim() : null;
    const internalPo = req.body.internal_po ? String(req.body.internal_po).trim() : null;
    const projectShort = req.body.project_short ? String(req.body.project_short).trim() : null;
    const requesterName = req.body.requester_name ? String(req.body.requester_name).trim() : null;

    const open = await pool.query(
      `SELECT id, start_ts FROM time_entries
       WHERE employee_id=$1 AND end_ts IS NULL AND start_ts IS NOT NULL
       ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true, start_time: open.rows[0].start_ts });

    const startTs = new Date();
    const workDate = toIsoDate(new Date());

    const ins = await pool.query(
      `
      INSERT INTO time_entries (employee_id, work_date, customer_po, internal_po, project_short, requester_name, start_ts)
      VALUES ($1, $2::date, $3, $4, $5, $6, $7)
      RETURNING start_ts
      `,
      [employeeId, workDate, customerPo, internalPo, projectShort, requesterName, startTs]
    );

    res.json({ ok: true, start_time: ins.rows[0].start_ts });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// BREAK: start
// ======================================================
app.post("/api/break/start", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id FROM break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true });

    await pool.query(
      `INSERT INTO break_entries (employee_id, start_ts) VALUES ($1, $2)`,
      [employeeId, new Date()]
    );

    res.json({ ok: true });
  } catch (e) {
    console.error("BREAK START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// BREAK: end
// ======================================================
app.post("/api/break/end", async (req, res) => {
  try {
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id, start_ts FROM break_entries WHERE employee_id=$1 AND end_ts IS NULL ORDER BY start_ts DESC LIMIT 1`,
      [employeeId]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Keine laufende Pause" });

    const endTs = new Date();
    const startTs = new Date(open.rows[0].start_ts);

    await pool.query(`UPDATE break_entries SET end_ts=$1 WHERE id=$2`, [endTs, open.rows[0].id]);

    const minutes = Math.max(0, Math.round((endTs.getTime() - startTs.getTime()) / 60000));
    res.json({ ok: true, minutes });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TIME: end (subtract breaks)
// ======================================================
app.post("/api/time/end", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();
    await ensureBreakEntriesSchema();

    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const activity = req.body.activity ? String(req.body.activity).trim() : "Arbeitszeit";

    const customerPo = req.body.customer_po ? String(req.body.customer_po).trim() : null;
    const internalPo = req.body.internal_po ? String(req.body.internal_po).trim() : null;
    const projectShort = req.body.project_short ? String(req.body.project_short).trim() : null;
    const requesterName = req.body.requester_name ? String(req.body.requester_name).trim() : null;

    const open = await pool.query(
      `
      SELECT id, start_ts
      FROM time_entries
      WHERE employee_id=$1 AND end_ts IS NULL AND start_ts IS NOT NULL
      ORDER BY start_ts DESC
      LIMIT 1
      `,
      [employeeId]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Kein laufender Arbeitsblock" });

    const endTs = new Date();
    const id = open.rows[0].id;
    const startTs = new Date(open.rows[0].start_ts);

    // close open breaks automatically at end time
    await pool.query(
      `UPDATE break_entries SET end_ts=$1 WHERE employee_id=$2 AND end_ts IS NULL`,
      [endTs, employeeId]
    );

    await pool.query(
      `
      UPDATE time_entries
      SET end_ts = $1,
          activity = $2,
          customer_po = COALESCE($3, customer_po),
          internal_po = COALESCE($4, internal_po),
          project_short = COALESCE($5, project_short),
          requester_name = COALESCE($6, requester_name)
      WHERE id = $7
      `,
      [endTs, activity, customerPo, internalPo, projectShort, requesterName, id]
    );

    const totalMs = Math.max(0, endTs.getTime() - startTs.getTime());

    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)) * 1000), 0)::bigint AS ms
      FROM break_entries
      WHERE employee_id = $1
        AND start_ts >= $2
        AND end_ts <= $3
        AND end_ts IS NOT NULL
      `,
      [employeeId, startTs, endTs]
    );

    const breakMs = Number(br.rows[0]?.ms || 0);
    const netMs = Math.max(0, totalMs - breakMs);

    res.json({
      ok: true,
      net_hours: (netMs / 3600000).toFixed(2),
      break_minutes: Math.round(breakMs / 60000),
    });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: time entries
// ======================================================
app.get("/api/debug/time-entries", async (req, res) => {
  try {
    await ensureTimeEntriesSchema();

    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `
      SELECT id, work_date, customer_po, internal_po, project_short, requester_name, start_ts, end_ts, activity
      FROM time_entries
      WHERE employee_id = $1
      ORDER BY start_ts DESC NULLS LAST, id DESC
      LIMIT 50
      `,
      [employeeId]
    );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("DEBUG TIME ENTRIES ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: staffplan rows on date
// ======================================================
app.get("/api/debug/staffplan-on-date", async (req, res) => {
  const date = String(req.query.date || "").trim();
  if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });

  const r = await pool.query(
    `
    SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
    FROM staffplan
    WHERE work_date = $1::date
    ORDER BY employee_name, customer_po, internal_po
    LIMIT 200
    `,
    [date]
  );

  res.json({ ok: true, date, rows: r.rows });
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
