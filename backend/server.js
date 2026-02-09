console.log("🔥🔥🔥 SERVER.JS + IMPORT HISTORY + ROLLBACK + DRY-RUN + STATS + SHAREPOINT + ABSENCES 🔥🔥🔥");

const path = require("path");
const fs = require("fs");
const crypto = require("crypto");
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");
const { downloadExcelFromShareLink } = require("./sharepoint");
const { buildErfassungsbogenPdf } = require("./a10/erfassungsbogenPdf");
const { loadStaffplanMapping } = require("./lib/staffplanProjectMapping");



const app = express();
// ======================================================
// CONFIG
// ======================================================
const PORT = process.env.PORT || 10000;
app.get("/api/_deploy_check", (req, res) => {
  res.json({ ok: true, msg: "deploy-check-2026-02-09-1" });
});


// ======================================================
// BASE MIDDLEWARES (einmalig!)
// ======================================================
app.use(cors());
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ======================================================
// SECURITY: Admin Code
// ======================================================
function requireCode2012(req) {
  const code =
    (req.query.code ||
      req.body?.code ||
      req.headers["x-admin-code"] ||
      "")
      .toString()
      .trim();

  if (code !== "2012") {
    const err = new Error("Falscher Sicherheitscode");
    err.status = 403;
    throw err;
  }
}
// ======================================================
// A9.9: Public cron trigger with secret token (stable for Render cron)
// GET /api/cron/run?token=...&mode=monthly|weekly
// ======================================================
app.get("/api/cron/run", async (req, res) => {
  try {
    const token = String(req.query.token || "").trim();
    const mode = String(req.query.mode || "").trim(); // monthly|weekly

    if (!process.env.CRON_TOKEN) {
      return res.status(500).json({ ok: false, error: "CRON_TOKEN not configured" });
    }
    if (token !== process.env.CRON_TOKEN) {
      return res.status(403).json({ ok: false, error: "forbidden" });
    }
    if (!["monthly", "weekly"].includes(mode)) {
      return res.status(400).json({ ok: false, error: "mode must be monthly|weekly" });
    }

    // Call the same logic as automation/run by performing an internal HTTP call to self:
    const base = `http://127.0.0.1:${PORT}`;
    const r = await fetch(`${base}/api/admin/automation/run`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "x-admin-code": "2012"
      },
      body: JSON.stringify({ mode, date: todayIsoBerlin() })
    });

    const text = await r.text();
    let j; try { j = JSON.parse(text); } catch { j = { ok: false, error: text }; }
    if (!r.ok || j.ok === false) return res.status(500).json({ ok: false, error: j.error || "automation failed", raw: j });

    return res.json({ ok: true, mode, ran_at: new Date().toISOString(), result: j });
  } catch (e) {
    console.error("CRON RUN ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});
app.get("/a10/erfassungsbogen.pdf", async (req, res) => {
  try {
    const group = ["date", "week", "project"].includes(req.query.group)
      ? req.query.group
      : "week";

    const showKwColumn = String(req.query.kw || "") === "1";

// Zeitraum (Pflicht)
const from = String(req.query.from || "").trim();
const to = String(req.query.to || "").trim();

if (
  !/^\d{4}-\d{2}-\d{2}$/.test(from) ||
  !/^\d{4}-\d{2}-\d{2}$/.test(to)
) {
  return res.status(400).json({
    ok: false,
    error: "Bitte from und to als YYYY-MM-DD setzen (z. B. ?from=2026-01-01&to=2026-01-31)",
  });
}

// optionale Filter
const customer_po = String(req.query.customer_po || "").trim() || null;
const internal_po = String(req.query.internal_po || "").trim() || null;
const project_short = String(req.query.project_short || "").trim() || null;

const { rows, meta } = await loadErfassungsbogenRows({
  from,
  to,
  customer_po,
  internal_po,
  project_short,
});

// Zeitraum-Label fürs PDF
const periodLabel = `Zeitraum: ${from} – ${to}`;

buildErfassungsbogenPdf(res, rows, {
  title: "Erfassungsbogen (Zeiten)",
  groupMode: group,
  periodLabel,
  logoPath: LOGO_FILE,
  showKwColumn,

  // 🚀 A10.3 aktiv
  employee_id,
  staffplanMap,

  meta: {
    customer: meta.customer || "—",
    customerPo: meta.customerPo,
    internalPo: meta.internalPo,
  },
});


} catch (e) {
  console.error("A10 PDF ERROR:", e);

  // Nur wenn du debug=1 & code=2012 setzt, geben wir Details zurück
  const debug = String(req.query.debug || "") === "1";
  const code = String(req.query.code || "").trim();

  if (debug && code === "2012") {
    return res.status(500).json({
      ok: false,
      error: e.message,
      hint: "Diese Debug-Ausgabe später wieder entfernen",
    });
  }

  return res.status(500).send("PDF generation failed");
}

}); 

// ======================================================
// ADMIN ROUTE GUARD (VARIANTE B)
// schützt automatisch alle /api/admin/* Endpunkte
// ======================================================
app.use("/api/admin", (req, res, next) => {
  try {
    requireCode2012(req);
    next();
  } catch (e) {
    res.status(e.status || 403).json({
      ok: false,
      error: e.message || "Admin-Zugriff verweigert",
    });
  }
});
console.log("🔐 Admin Route Guard aktiv");
// ======================================================
// ADMIN DEBUG: A10 PO Sample
// ======================================================
app.get("/api/admin/debug/a10-po-sample", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();

    const q = await pool.query(
      `
      WITH te_base AS (
        SELECT te.employee_id, te.work_date::date AS work_date
        FROM time_entries te
        WHERE te.work_date BETWEEN $1::date AND $2::date
          AND te.start_ts IS NOT NULL AND te.end_ts IS NOT NULL
      ),
      te_proj AS (
        SELECT
          b.employee_id,
          b.work_date,
          (
            SELECT NULLIF(TRIM(e.project_id), '')
            FROM time_events e
            WHERE e.employee_id = b.employee_id
              AND (e.event_time AT TIME ZONE 'Europe/Berlin')::date = b.work_date
              AND e.event_type='clock_in'
              AND e.project_id IS NOT NULL
            ORDER BY e.event_time DESC
            LIMIT 1
          ) AS project_id
        FROM te_base b
      )
      SELECT
        b.work_date,
        b.employee_id,
        tp.project_id,
        p.customer_po AS projects_customer_po,
        p.internal_po AS projects_internal_po,
        p.customer   AS projects_customer
      FROM te_base b
      LEFT JOIN te_proj tp
        ON tp.employee_id=b.employee_id AND tp.work_date=b.work_date
      LEFT JOIN projects p
        ON TRIM(p.project_id)=TRIM(tp.project_id)
      ORDER BY b.work_date ASC, b.employee_id ASC
      LIMIT 50
      `,
      [from, to]
    );

    return res.json({ ok: true, rows: q.rows });
  } catch (e) {
    console.error("A10 PO SAMPLE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});
app.get("/api/admin/debug/po-check", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const customer_po = String(req.query.customer_po || "").trim();

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "from/to YYYY-MM-DD required" });
    }
    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po required" });

    // 1) Gibt es staffplan-Zeilen für diese PO?
    const sp = await pool.query(
      `
      SELECT COUNT(*)::int AS cnt
      FROM staffplan
      WHERE work_date BETWEEN $1::date AND $2::date
        AND regexp_replace(COALESCE(customer_po,''), '\\s', '', 'g')
            = regexp_replace($3, '\\s', '', 'g')
      `,
      [from, to, customer_po]
    );


    // 2) Gibt es time_entries im Zeitraum?
    const te = await pool.query(
      `
      SELECT COUNT(*)::int AS cnt
      FROM time_entries
      WHERE work_date BETWEEN $1::date AND $2::date
        AND start_ts IS NOT NULL AND end_ts IS NOT NULL
      `,
      [from, to]
    );

    // 3) Gibt es JOIN-Treffer (time_entries <-> staffplan)?
    const joinCnt = await pool.query(
      `
      SELECT COUNT(*)::int AS cnt
      FROM time_entries te
      JOIN staffplan sp
        ON sp.employee_id = te.employee_id
       AND sp.work_date = te.work_date
      WHERE te.work_date BETWEEN $1::date AND $2::date
        AND regexp_replace(COALESCE(sp.customer_po,''), '\\s', '', 'g')
            = regexp_replace($3, '\\s', '', 'g')
      `,
      [from, to, customer_po]
    );

    return res.json({
      ok: true,
      staffplan_po_rows: sp.rows[0].cnt,
      time_entries_rows: te.rows[0].cnt,
      join_rows_for_po: joinCnt.rows[0].cnt,
    });
  } catch (e) {
    console.error("PO CHECK ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});
app.get("/api/admin/debug/time-entries-sample", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "from/to YYYY-MM-DD required" });
    }

    const r = await pool.query(
      `
      SELECT
        te.employee_id,
        te.work_date::date AS work_date,
        te.start_ts,
        te.end_ts,
        te.break_minutes,
        te.auto_break_minutes,
        COALESCE(
          0,
          FLOOR(
            (EXTRACT(EPOCH FROM (te.end_ts - te.start_ts)) / 60.0)
            - COALESCE(te.break_minutes,0)
            - COALESCE(te.auto_break_minutes,0)
          )
        )::int AS minutes,
        sp.customer_po,
        sp.internal_po,
        sp.project_short,
        sp.customer
      FROM time_entries te
      LEFT JOIN staffplan sp
        ON sp.employee_id = te.employee_id
       AND sp.work_date = te.work_date
      WHERE te.work_date BETWEEN $1::date AND $2::date
        AND te.start_ts IS NOT NULL AND te.end_ts IS NOT NULL
      ORDER BY te.work_date ASC
      LIMIT 50
      `,
      [from, to]
    );

    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("TIME ENTRIES SAMPLE ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});


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
  host: process.env.PGHOST,
  port: Number(process.env.PGPORT || 5432),
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  ssl: { rejectUnauthorized: false },
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

function todayIsoBerlin() {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return fmt.format(new Date()); // YYYY-MM-DD
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
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

function commaSwapName(s) {
  const t = String(s || "").trim();
  if (!t.includes(",")) return t;
  const last = t.split(",")[0].trim();
  const first = t.split(",").slice(1).join(",").trim();
  if (!first || !last) return t;
  return `${first} ${last}`.replace(/\s+/g, " ").trim();
}

function makeAutoIdFromName(name) {
  const n = normalizeName(name);
  let h = 0;
  for (let i = 0; i < n.length; i++) h = (h * 31 + n.charCodeAt(i)) >>> 0;
  return "AUTO_" + h.toString(36);
}

function sha256Hex(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

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

// ======================================================
// DB HELPERS
// ======================================================
async function ensureColumn(table, column, typeSql) {
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='${table}'
          AND column_name='${column}'
      ) THEN
        ALTER TABLE ${table}
        ADD COLUMN ${column} ${typeSql};
      END IF;
    END $$;
  `);
}
async function loadErfassungsbogenRows({ from, to, customer_po, internal_po, project_short }) {
  const params = [from, to];
  let where = `b.work_date BETWEEN $1::date AND $2::date`;

  if (customer_po) {
    params.push(customer_po);
    where += ` AND regexp_replace(COALESCE(NULLIF(TRIM(sp.customer_po), ''), NULLIF(TRIM(p.customer_po), ''), ''), '\\s', '', 'g')
                 = regexp_replace($${params.length}, '\\s', '', 'g')`;
  }
  if (internal_po) {
    params.push(internal_po);
    where += ` AND regexp_replace(COALESCE(NULLIF(TRIM(sp.internal_po), ''), NULLIF(TRIM(p.internal_po), ''), ''), '\\s', '', 'g')
                 = regexp_replace($${params.length}, '\\s', '', 'g')`;
  }
  if (project_short) {
    params.push(project_short);
    where += ` AND TRIM(COALESCE(NULLIF(TRIM(sp.project_short), ''), NULLIF(TRIM(tp.project_id), ''), '')) = TRIM($${params.length})`;
  }

  const sql = `
  WITH te_base AS (
    SELECT
      te.employee_id,
      te.work_date::date AS work_date,
      te.start_ts,
      te.end_ts,
      te.break_minutes,
      te.auto_break_minutes
    FROM time_entries te
  ),
  te_proj AS (
    SELECT
      b.employee_id,
      b.work_date,
      (
        SELECT NULLIF(TRIM(e.project_id), '')
        FROM time_events e
        WHERE e.employee_id = b.employee_id
          AND (e.event_time AT TIME ZONE 'Europe/Berlin')::date = b.work_date
          AND e.event_type = 'clock_in'
          AND e.project_id IS NOT NULL
        ORDER BY e.event_time DESC
        LIMIT 1
      ) AS project_id
    FROM te_base b
  ),
  base_rows AS (
    SELECT
      b.work_date::date AS work_date,

      COALESCE(
        NULLIF(TRIM(sp.project_short), ''),
        NULLIF(TRIM(tp.project_id), ''),
        '—'
      ) AS project,

      COALESCE(
        NULLIF(TRIM(sp.internal_po), ''),
        NULLIF(TRIM(p.internal_po), '')
      ) AS internal_po,

      COALESCE(
        NULLIF(TRIM(sp.customer_po), ''),
        NULLIF(TRIM(p.customer_po), '')
      ) AS customer_po,

      COALESCE(
        NULLIF(TRIM(sp.customer), ''),
        NULLIF(TRIM(p.customer), ''),
        NULL
      ) AS customer,

      GREATEST(
        0,
        FLOOR(
          (EXTRACT(EPOCH FROM (b.end_ts - b.start_ts)) / 60.0)
          - COALESCE(b.break_minutes, 0)
          - COALESCE(b.auto_break_minutes, 0)
        )
      )::int AS minutes

    FROM te_base b
    LEFT JOIN staffplan sp
      ON sp.employee_id = b.employee_id
     AND sp.work_date = b.work_date

    LEFT JOIN te_proj tp
      ON tp.employee_id = b.employee_id
     AND tp.work_date = b.work_date

    LEFT JOIN projects p
      ON TRIM(p.project_id) = TRIM(tp.project_id)

    WHERE ${where}
      AND b.start_ts IS NOT NULL
      AND b.end_ts IS NOT NULL
  )
  SELECT
    work_date,
    project,
    internal_po,
    customer_po,
    customer,
    SUM(minutes)::int AS minutes
  FROM base_rows
  GROUP BY work_date, project, internal_po, customer_po, customer
  ORDER BY work_date ASC, project ASC, internal_po ASC
`;


  const r = await pool.query(sql, params);

  const rows = r.rows.map((x) => ({
    date: String(x.work_date).slice(0, 10), // YYYY-MM-DD
    project: x.project || "—",
    internal_po: x.internal_po || null,
    task: null,
    minutes: Number(x.minutes || 0),
  }));
// A10.3 – staffplan mapping (latest staffplan wins)
const staffplanMap = await loadStaffplanMapping(db, { from, to });

  const meta = {
    customer: null,
    customerPo: customer_po || null,
    internalPo: internal_po || null,
  };

  const customers = Array.from(new Set(r.rows.map((x) => x.customer).filter(Boolean)));
  if (customers.length === 1) meta.customer = customers[0];

  return { rows, meta };
}

// -------- Settings helpers --------
async function getSetting(key) {
  const r = await pool.query(`SELECT value FROM app_settings WHERE key=$1`, [key]);
  return r.rowCount ? r.rows[0].value : null;
}

async function setSetting(key, value) {
  await pool.query(
    `
    INSERT INTO app_settings (key, value, updated_at)
    VALUES ($1, $2, NOW())
    ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
    `,
    [key, value]
  );
}

async function ensureEmployeeExists(employee_id) {
  // Minimal: wenn nicht vorhanden, anlegen
  // Name = employee_id (kann später über Admin korrigiert werden)
  await pool.query(
    `
    INSERT INTO employees (employee_id, name)
    VALUES ($1, $2)
    ON CONFLICT (employee_id) DO NOTHING
    `,
    [employee_id, employee_id]
  );
}

// ======================================================
// MIGRATE
// ======================================================
async function migrate() {
  console.log("🔧 DB migrate start");

  // ===== Core tables =====
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de',
      weekly_hours NUMERIC DEFAULT 40
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS projects (
      project_id TEXT PRIMARY KEY,
      customer_po TEXT,
      internal_po TEXT,
      customer TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

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

  // legacy-sicher
  await pool.query(`
    ALTER TABLE employees
    ADD COLUMN IF NOT EXISTS weekly_hours NUMERIC DEFAULT 40;
  `);

  // ======================================================
  // A9.12: Automation run log (notifications)
  // ======================================================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS automation_runs (
      id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      mode TEXT NOT NULL,
      period_start DATE NOT NULL,
      period_end DATE NOT NULL,
      created_count INT NOT NULL DEFAULT 0,
      skipped_count INT NOT NULL DEFAULT 0,
      created_json JSONB,
      skipped_json JSONB,
      note TEXT
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS automation_runs_by_created_at
    ON automation_runs (created_at DESC);
  `);

  // ======================================================
  // INVOICES (A8 – Abrechnung & Go-Live)
  // ======================================================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoices (
      id BIGSERIAL PRIMARY KEY,
      invoice_number TEXT UNIQUE,
      customer_po TEXT NOT NULL,
      customer TEXT,
      period_start DATE NOT NULL,
      period_end DATE NOT NULL,
      status TEXT NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft','final','exported')),
      currency TEXT DEFAULT 'EUR',
      total_amount NUMERIC,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      finalized_at TIMESTAMPTZ
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_lines (
      id BIGSERIAL PRIMARY KEY,
      invoice_id BIGINT NOT NULL
        REFERENCES invoices(id) ON DELETE CASCADE,
      description TEXT NOT NULL,
      quantity NUMERIC,
      unit TEXT,
      unit_price NUMERIC,
      amount NUMERIC NOT NULL
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_counters (
      year INT PRIMARY KEY,
      last_number INT NOT NULL
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS invoice_lines_by_invoice
    ON invoice_lines (invoice_id);
  `);

  // ======================================================
  // A8.11: Invoice export tracking
  // ======================================================
  await ensureColumn("invoices", "exported_at", "TIMESTAMPTZ");
  await ensureColumn("invoices", "export_note", "TEXT");

  // ======================================================
  // A9.10: Prevent duplicate invoices (source + unique key)
  // ======================================================
  await ensureColumn("invoices", "source", "TEXT");

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS invoices_uniq_po_period_source
    ON invoices (customer_po, period_start, period_end, COALESCE(source,''));
  `);

  // Backfill: set source for existing rows where NULL/empty
  await pool.query(`
    UPDATE invoices
    SET source = 'legacy'
    WHERE source IS NULL OR source = '';
  `);

  // Dedupe invoices for same (customer_po, period_start, period_end, source)
  // Keep the newest (highest id), delete older ones + their lines cascade
  await pool.query(`
    WITH ranked AS (
      SELECT
        id,
        ROW_NUMBER() OVER (
          PARTITION BY customer_po, period_start, period_end, COALESCE(source,'')
          ORDER BY id DESC
        ) AS rn
      FROM invoices
    )
    DELETE FROM invoices i
    USING ranked r
    WHERE i.id = r.id
      AND r.rn > 1;
  `);

  // ===== STAFFPLAN: Duplikate entfernen + Unique Index (NULL-sicher) =====
  try {
    await pool.query(`DROP INDEX IF EXISTS staffplan_uniq;`);
  } catch (e) {
    // ignore
  }

  try {
    const dedupe = await pool.query(`
      WITH ranked AS (
        SELECT id,
          ROW_NUMBER() OVER (
            PARTITION BY
              employee_id,
              work_date,
              COALESCE(customer_po,''),
              COALESCE(internal_po,''),
              COALESCE(project_short,'')
            ORDER BY id DESC
          ) AS rn
        FROM staffplan
      )
      DELETE FROM staffplan s
      USING ranked r
      WHERE s.id = r.id
        AND r.rn > 1
      RETURNING s.id;
    `);

    console.log("🧹 staffplan dedupe deleted:", dedupe.rowCount);
  } catch (e) {
    console.warn("⚠️ staffplan dedupe/index skipped:", e.code || e.message);
  }

  try {
    await pool.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS staffplan_uniq2
      ON staffplan (
        employee_id,
        work_date,
        COALESCE(customer_po,''),
        COALESCE(internal_po,''),
        COALESCE(project_short,'')
      );
    `);

    console.log("✅ staffplan_uniq2 aktiv");
  } catch (e) {
    console.warn("⚠️ staffplan dedupe/index skipped:", e.code || e.message);
  }

  // normale Indizes (IMMER ausführen)
  await pool.query(`
    CREATE INDEX IF NOT EXISTS staffplan_by_date
    ON staffplan (work_date);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS staffplan_by_date_emp
    ON staffplan (work_date, employee_id);
  `);

  // ======================================================
  // PO WORK RULES
  // ======================================================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS po_work_rules (
      id BIGSERIAL PRIMARY KEY,
      customer_po TEXT NOT NULL,
      weekday INT NOT NULL CHECK (weekday BETWEEN 1 AND 7),
      start_time TIME NOT NULL,
      grace_minutes INT NOT NULL DEFAULT 0 CHECK (grace_minutes BETWEEN 0 AND 120),
      bill_travel boolean NOT NULL DEFAULT false,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (customer_po, weekday)
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS po_work_rules_po_day
    ON po_work_rules (customer_po, weekday);
  `);

  // ===== Import History tables =====
  await pool.query(`
    CREATE TABLE IF NOT EXISTS import_runs (
      run_id BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      finished_at TIMESTAMPTZ,
      status TEXT NOT NULL DEFAULT 'running',
      mode TEXT NOT NULL,
      filename TEXT,
      file_sha256 TEXT,
      target_end DATE,
      date_from DATE,
      date_to DATE,
      date_cols INT,
      imported INT DEFAULT 0,
      inserted_rows INT DEFAULT 0,
      updated_rows INT DEFAULT 0,
      skipped_no_employee_rows INT DEFAULT 0,
      note TEXT,
      actor_ip TEXT,
      rolled_back_at TIMESTAMPTZ,
      rollback_note TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan_changes (
      change_id BIGSERIAL PRIMARY KEY,
      run_id BIGINT NOT NULL REFERENCES import_runs(run_id) ON DELETE CASCADE,
      change_type TEXT NOT NULL,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      internal_po TEXT,
      project_short TEXT,
      old_employee_name TEXT,
      old_requester_name TEXT,
      old_calendar_week TEXT,
      old_customer TEXT,
      old_planned_hours NUMERIC
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS staffplan_changes_by_run
    ON staffplan_changes (run_id, change_id);
  `);

  // ===== App Settings =====
  await pool.query(`
    CREATE TABLE IF NOT EXISTS app_settings (
      key TEXT PRIMARY KEY,
      value TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  // ===== Employee absences =====
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employee_absences (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL REFERENCES employees(employee_id) ON DELETE CASCADE,
      type TEXT NOT NULL CHECK (type IN ('sick','vacation')),
      date_from DATE NOT NULL,
      date_to   DATE NOT NULL,
      note TEXT,
      status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','cancelled')),
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS employee_absences_by_emp_dates
    ON employee_absences (employee_id, date_from, date_to);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS employee_absences_by_type_dates
    ON employee_absences (type, date_from, date_to);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS employee_absences_active
    ON employee_absences (employee_id)
    WHERE status='active';
  `);

  console.log("✅ DB migrate finished");
}

// ======================================================
// STATIC
// ======================================================
// Explizite Routes zuerst, damit /admin zuverlässig funktioniert
app.get("/", (req, res) => res.redirect("/admin"));
app.get("/admin", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "admin.html")));
app.get("/debug.html", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "debug.html")));

// Danach static (ohne auto-index)
app.use(express.static(FRONTEND_DIR, { index: false }));

// ======================================================
// HEALTH + BUILD
// ======================================================
app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/api/debug/build", (req, res) => {
  res.json({
    ok: true,
    build: "server.js + IMPORT HISTORY + ROLLBACK + DRY-RUN + STATS + SHAREPOINT + ABSENCES",
    node: process.version,
    now: new Date().toISOString(),
  });
});

// ✅ Beweis-Endpunkt: zeigt sicher, ob diese server.js wirklich deployed ist
app.get("/api/debug/has-logo-route", (req, res) => {
  res.json({ ok: true, hasLogoRoute: true });
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
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    // minimaler Guard (ohne große Umstellung)
    const mime = String(req.file.mimetype || "");
    if (mime && mime !== "image/png") {
      return res.status(400).json({ ok: false, error: "Nur PNG erlaubt" });
    }

    // nicht-blockierend vermeiden wäre async; 1:1 minimal bleibt sync, aber try/catch
    fs.writeFileSync(LOGO_FILE, req.file.buffer);
    return res.json({ ok: true });
  } catch (e) {
    console.error("LOGO UPLOAD ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// SETTINGS: SharePoint Link + Status
// ======================================================
app.get("/api/settings/staffplan-sharelink", async (req, res) => {
  try {
    const url = await getSetting("staffplan_sharelink");
    const lastHash = await getSetting("staffplan_last_sha256");
    const lastRunId = await getSetting("staffplan_last_run_id");
    const lastAt = await getSetting("staffplan_last_import_at");
    return res.json({ ok: true, url, last_hash: lastHash, last_run_id: lastRunId, last_import_at: lastAt });
  } catch (e) {
    console.error("SETTINGS GET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/settings/staffplan-sharelink", async (req, res) => {
  try {
    const url = String(req.body?.url || "").trim();
    if (!url.startsWith("https://")) return res.status(400).json({ ok: false, error: "URL ungültig" });
    await setSetting("staffplan_sharelink", url);
    return res.json({ ok: true });
  } catch (e) {
    console.error("SETTINGS POST ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: scan-dates
// ======================================================
app.post("/api/debug/scan-dates", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    let headerRow = null,
      bestCnt = 0,
      bestStartCol = null,
      bestEndCol = null;

    for (let rr = 0; rr <= 25; rr++) {
      let cnt = 0,
        first = null,
        last = null;
      for (let c = 0; c <= 450; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: rr, c })];
        const d = parseExcelDate(cell);
        if (d) {
          cnt++;
          if (first === null) first = c;
          last = c;
        }
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = rr;
        bestStartCol = first;
        bestEndCol = last;
      }
    }

    if (headerRow === null || bestCnt < 3 || bestStartCol === null || bestEndCol === null) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden" });
    }

    const startCol = bestStartCol;
    const endCol = bestEndCol;

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

    if (!baseDate || firstDateCol === null) {
      return res.json({ ok: false, error: "Kein erstes Datum parsebar" });
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
        header_raw: cell?.w ?? cell?.v ?? null,
        parsed_from_header: !!parsed,
      });
    }

    return res.json({
      ok: true,
      sheet: wb.SheetNames[0],
      header_row_1based: headerRow + 1,
      start_col: startCol,
      end_col: endCol,
      date_cols: dates.length,
      date_from: dates[0]?.iso,
      date_to: dates[dates.length - 1]?.iso,
      tail: dates.slice(-15),
    });
  } catch (e) {
    console.error("SCAN DATES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: scan-values
// ======================================================
app.post("/api/debug/scan-values", upload.single("file"), async (req, res) => {
  try {
    const targetIso = String(req.query.target || "2025-12-27").trim();
    if (!/^\d{4}-\d{2}-\d{2}$/.test(targetIso)) {
      return res.status(400).json({ ok: false, error: "target muss YYYY-MM-DD sein" });
    }
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    let headerRow = null,
      bestCnt = 0,
      bestStartCol = null,
      bestEndCol = null;

    for (let rr = 0; rr <= 25; rr++) {
      let cnt = 0,
        first = null,
        last = null;
      for (let c = 0; c <= 450; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: rr, c })];
        const d = parseExcelDate(cell);
        if (d) {
          cnt++;
          if (first === null) first = c;
          last = c;
        }
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = rr;
        bestStartCol = first;
        bestEndCol = last;
      }
    }

    if (headerRow === null || bestCnt < 3 || bestStartCol === null || bestEndCol === null) {
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden" });
    }

    const startCol = bestStartCol;
    const endCol = bestEndCol;

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
    if (!baseDate || firstDateCol === null) {
      return res.json({ ok: false, error: "Kein erstes Datum parsebar" });
    }

    const baseIso = toIsoDate(baseDate);
    const base = new Date(baseIso + "T00:00:00.000Z");
    const target = new Date(targetIso + "T00:00:00.000Z");
    const diffDays = Math.round((target.getTime() - base.getTime()) / 86400000);
    const targetCol = firstDateCol + diffDays;

    const headerCell = ws[XLSX.utils.encode_cell({ r: headerRow, c: targetCol })];
    const headerRaw = headerCell?.w ?? headerCell?.v ?? null;

    let hits = [];
    for (let r = 5; r < 500; r += 2) {
      const empCellK = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      const empCellI = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const empName =
        (empCellK?.v ? String(empCellK.v).trim() : "") ||
        (empCellI?.v ? String(empCellI.v).trim() : "");
      if (!empName) continue;

      const projCell = ws[XLSX.utils.encode_cell({ r, c: targetCol })];
      const planCell = ws[XLSX.utils.encode_cell({ r: r + 1, c: targetCol })];

      const proj = projCell?.v ?? null;
      const plan = planCell?.v ?? null;

      if ((proj !== null && String(proj).trim() !== "") || (typeof plan === "number" && isFinite(plan))) {
        hits.push({ row: r + 1, employee: empName, proj, plan });
        if (hits.length >= 25) break;
      }
    }

    return res.json({
      ok: true,
      sheet: wb.SheetNames[0],
      header_row_1based: headerRow + 1,
      first_date_iso: baseIso,
      first_date_col: firstDateCol,
      target: targetIso,
      target_col: targetCol,
      target_header_raw: headerRaw,
      found_rows_with_values: hits.length,
      sample_hits: hits,
      note: "Wenn found_rows_with_values=0, sind an diesem Datum in den erwarteten Zellen keine Werte vorhanden (proj/plan).",
    });
  } catch (e) {
    console.error("SCAN VALUES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// IMPORT HISTORY API
// ======================================================
app.get("/api/import/history", async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(200, parseInt(req.query.limit || "50", 10) || 50));
    const r = await pool.query(
      `
      SELECT run_id, created_at, finished_at, status, mode, filename, file_sha256,
             target_end, date_from, date_to, date_cols,
             imported, inserted_rows, updated_rows, skipped_no_employee_rows,
             rolled_back_at, note
      FROM import_runs
      ORDER BY run_id DESC
      LIMIT $1
      `,
      [limit]
    );
    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("IMPORT HISTORY ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/import/history/:run_id", async (req, res) => {
  try {
    const runId = String(req.params.run_id || "").trim();
    const run = await pool.query(`SELECT * FROM import_runs WHERE run_id=$1`, [runId]);
    if (!run.rowCount) return res.status(404).json({ ok: false, error: "run_id nicht gefunden" });

    const ch = await pool.query(
      `
      SELECT change_type, COUNT(*)::int AS cnt
      FROM staffplan_changes
      WHERE run_id=$1
      GROUP BY change_type
      `,
      [runId]
    );

    return res.json({ ok: true, run: run.rows[0], change_counts: ch.rows });
  } catch (e) {
    console.error("IMPORT HISTORY RUN ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ROLLBACK API
// ======================================================
app.post("/api/import/rollback", async (req, res) => {
  const runId = String(req.body?.run_id ?? "").trim();
  if (!runId) return res.status(400).json({ ok: false, error: "run_id fehlt" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const run = await client.query(`SELECT * FROM import_runs WHERE run_id=$1 FOR UPDATE`, [runId]);
    if (!run.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "run_id nicht gefunden" });
    }
    const runRow = run.rows[0];

    if (runRow.mode !== "write") {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "Rollback nur für mode=write möglich" });
    }
    if (runRow.status !== "ok") {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "Rollback nur möglich, wenn status=ok" });
    }
    if (runRow.rolled_back_at) {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "Dieser Run wurde bereits gerollbackt" });
    }

    const changes = await client.query(
      `
      SELECT *
      FROM staffplan_changes
      WHERE run_id=$1
      ORDER BY change_id DESC
      `,
      [runId]
    );

    let deleted = 0;
    let restored = 0;

    for (const c of changes.rows) {
      if (c.change_type === "insert") {
        const del = await client.query(
          `
          DELETE FROM staffplan
          WHERE employee_id=$1
            AND work_date=$2::date
            AND COALESCE(customer_po,'')=COALESCE($3,'')
            AND COALESCE(internal_po,'')=COALESCE($4,'')
            AND COALESCE(project_short,'')=COALESCE($5,'')
          `,
          [c.employee_id, c.work_date, c.customer_po, c.internal_po, c.project_short]
        );
        deleted += del.rowCount;
      } else if (c.change_type === "update") {
        const upd = await client.query(
          `
          UPDATE staffplan
          SET employee_name=$1,
              requester_name=$2,
              calendar_week=$3,
              customer=$4,
              planned_hours=$5
          WHERE employee_id=$6
            AND work_date=$7::date
            AND COALESCE(customer_po,'')=COALESCE($8,'')
            AND COALESCE(internal_po,'')=COALESCE($9,'')
            AND COALESCE(project_short,'')=COALESCE($10,'')
          `,
          [
            c.old_employee_name,
            c.old_requester_name,
            c.old_calendar_week,
            c.old_customer,
            c.old_planned_hours,
            c.employee_id,
            c.work_date,
            c.customer_po,
            c.internal_po,
            c.project_short,
          ]
        );
        restored += upd.rowCount;
      }
    }

    await client.query(
      `UPDATE import_runs SET rolled_back_at=NOW(), rollback_note=$2 WHERE run_id=$1`,
      [runId, `rollback via api, deleted=${deleted}, restored=${restored}`]
    );

    await client.query("COMMIT");
    return res.json({ ok: true, run_id: runId, deleted_inserts: deleted, restored_updates: restored });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("ROLLBACK ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// ======================================================
// IMPORT CORE (Upload + SharePoint use same function)
// ======================================================
async function doImportStaffplan({
  buffer,
  originalname = "staffplan.xlsx",
  dryRun = false,
  reset = false,
  targetEndIso = null,
  actorIp = null,
}) {
  const startedAt = Date.now();

  let runId = null;
  const fileHash = buffer ? sha256Hex(buffer) : null;

  // history run insert
  try {
    const rr = await pool.query(
      `
      INSERT INTO import_runs (mode, filename, file_sha256, target_end, actor_ip, status, note)
      VALUES ($1,$2,$3,$4::date,$5,'running',$6)
      RETURNING run_id
      `,
      [
        dryRun ? "dry_run" : "write",
        originalname,
        fileHash,
        targetEndIso,
        actorIp,
        dryRun ? "dry-run (no db write)" : "write import",
      ]
    );
    runId = rr.rows[0].run_id;
  } catch (e) {
    console.error("IMPORT_RUNS INSERT ERROR:", e);
  }

  const client = await pool.connect();
  try {
    if (!buffer || !buffer.length) throw new Error("Leerer Datei-Buffer");

    if (!dryRun) await client.query("BEGIN");

    const wb = XLSX.read(buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    // Header finden
    let headerRow = null,
      bestCnt = 0,
      bestStartCol = null,
      bestEndCol = null;

    for (let rr = 0; rr <= 25; rr++) {
      let cnt = 0,
        first = null,
        last = null;
      for (let c = 0; c <= 450; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: rr, c })];
        const d = parseExcelDate(cell);
        if (d) {
          cnt++;
          if (first === null) first = c;
          last = c;
        }
      }
      if (cnt > bestCnt) {
        bestCnt = cnt;
        headerRow = rr;
        bestStartCol = first;
        bestEndCol = last;
      }
    }

    if (headerRow === null || bestCnt < 3 || bestStartCol === null || bestEndCol === null) {
      throw new Error("Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..26)");
    }

    const startCol = bestStartCol;
    let endCol = bestEndCol;

    // erstes Datum finden
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
    if (!baseDate || firstDateCol === null) throw new Error("Header gefunden, aber kein erstes Datum parsebar");

    // ensure endCol covers target_end column (Mindestziel)
    if (targetEndIso && /^\d{4}-\d{2}-\d{2}$/.test(targetEndIso)) {
      const baseIso = toIsoDate(baseDate);
      const base = new Date(baseIso + "T00:00:00.000Z");
      const targetEnd = new Date(targetEndIso + "T00:00:00.000Z");
      const diffDays = Math.round((targetEnd.getTime() - base.getTime()) / 86400000);
      const targetCol = firstDateCol + diffDays;
      if (targetCol > endCol) endCol = targetCol;
    }

    // dates bauen
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
      const d = parsed ? parsed : new Date(currentBaseDate.getTime() + (c - currentBaseCol) * 86400000);
      dates.push({ col: c, iso: toIsoDate(d), cw: "CW" + getISOWeek(d) });
    }

    // Reset nur wenn NICHT dry-run
    if (!dryRun && reset) {
      await client.query("TRUNCATE staffplan");
    }

    // DRY-RUN: existing keys im Date-Range laden
    let existingKeySet = null;
    if (dryRun) {
      existingKeySet = new Set();
      const rExist = await client.query(
        `
        SELECT employee_id, work_date, COALESCE(customer_po,'') AS customer_po,
               COALESCE(internal_po,'') AS internal_po,
               COALESCE(project_short,'') AS project_short
        FROM staffplan
        WHERE work_date BETWEEN $1::date AND $2::date
        `,
        [dates[0].iso, dates[dates.length - 1].iso]
      );
      for (const row of rExist.rows) {
        const k = `${row.employee_id}#${toIsoDate(new Date(row.work_date))}#${row.customer_po}#${row.internal_po}#${row.project_short}`;
        existingKeySet.add(k);
      }
    }

    // Counters
    let imported = 0;
    let inserted_rows = 0;
    let updated_rows = 0;

    let would_write_rows = 0;
    let would_insert_rows = 0;
    let would_update_rows = 0;

    let skippedNoEmployee = 0;

    // OPT: stop after long empty streak
    const EMPTY_STREAK_BREAK = 200;
    let emptyEmployeeStreak = 0;
    let lastProcessedRow = null;

    for (let r = 5; r < 20000; r += 2) {
      lastProcessedRow = r;

      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      const empCellK = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      const empCellI = ws[XLSX.utils.encode_cell({ r, c: 8 })];

      const employeeNameRaw =
        (empCellK?.v ? String(empCellK.v).trim() : "") ||
        (empCellI?.v ? String(empCellI.v).trim() : "");

      if (!employeeNameRaw) {
        skippedNoEmployee++;
        emptyEmployeeStreak++;
        if (emptyEmployeeStreak >= EMPTY_STREAK_BREAK) break;
        continue;
      }
      emptyEmployeeStreak = 0;

      const employeeNameCanonical = commaSwapName(employeeNameRaw);

      // employee_id (dry-run legt keine employees an)
      const n1 = normalizeName(employeeNameRaw);
      const n2 = normalizeName(employeeNameCanonical);

      const emp = await client.query(
        `
        SELECT employee_id
        FROM employees
        WHERE lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) = $1
           OR lower(regexp_replace(trim(name), '\\s+', ' ', 'g')) = $2
        LIMIT 1
        `,
        [n1, n2]
      );

      let employeeId;
      if (emp.rowCount) {
        employeeId = emp.rows[0].employee_id;
      } else {
        employeeId = makeAutoIdFromName(employeeNameCanonical);
        if (!dryRun) {
          await client.query(
            `
            INSERT INTO employees (employee_id, name)
            VALUES ($1, $2)
            ON CONFLICT (employee_id) DO UPDATE SET name = EXCLUDED.name
            `,
            [employeeId, employeeNameCanonical]
          );
        }
      }

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 6 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = (typeof planRaw === "number" && isFinite(planRaw)) ? planRaw : null;

        if (!proj && plan === null) continue;

        const key = `${employeeId}#${d.iso}#${customerPo || ""}#${internalPo || ""}#${proj || ""}`;

        if (dryRun) {
          would_write_rows++;
          if (existingKeySet && existingKeySet.has(key)) would_update_rows++;
          else would_insert_rows++;
          continue;
        }

        // ===== HISTORY: check existing row BEFORE upsert (needed for rollback of updates)
        const existing = await client.query(
          `
          SELECT employee_name, requester_name, calendar_week, customer, planned_hours
          FROM staffplan
          WHERE employee_id=$1
            AND work_date=$2::date
            AND COALESCE(customer_po,'')=COALESCE($3,'')
            AND COALESCE(internal_po,'')=COALESCE($4,'')
            AND COALESCE(project_short,'')=COALESCE($5,'')
          LIMIT 1
          `,
          [employeeId, d.iso, customerPo, internalPo, proj]
        );

        const existedBefore = existing.rowCount > 0;

        // UPSERT + inserted flag
        const q = await client.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          ON CONFLICT (
            employee_id,
            work_date,
            (COALESCE(customer_po,'')),
            (COALESCE(internal_po,'')),
            (COALESCE(project_short,''))
          )
          DO UPDATE SET
            employee_name  = EXCLUDED.employee_name,
            requester_name = EXCLUDED.requester_name,
            calendar_week  = EXCLUDED.calendar_week,
            customer       = EXCLUDED.customer,
            planned_hours  = EXCLUDED.planned_hours
          RETURNING (xmax = 0) AS inserted
          `,
          [
            employeeId,
            employeeNameCanonical,
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
        const inserted = !!q.rows[0]?.inserted;

        if (inserted) inserted_rows++;
        else updated_rows++;

        // ===== record changes
        if (runId) {
          if (inserted && !existedBefore) {
            await client.query(
              `
              INSERT INTO staffplan_changes
                (run_id, change_type, employee_id, work_date, customer_po, internal_po, project_short)
              VALUES ($1,'insert',$2,$3::date,$4,$5,$6)
              `,
              [runId, employeeId, d.iso, customerPo, internalPo, proj]
            );
          } else if (!inserted && existedBefore) {
            const old = existing.rows[0];
            await client.query(
              `
              INSERT INTO staffplan_changes
                (run_id, change_type, employee_id, work_date, customer_po, internal_po, project_short,
                 old_employee_name, old_requester_name, old_calendar_week, old_customer, old_planned_hours)
              VALUES ($1,'update',$2,$3::date,$4,$5,$6,$7,$8,$9,$10,$11)
              `,
              [
                runId,
                employeeId,
                d.iso,
                customerPo,
                internalPo,
                proj,
                old.employee_name,
                old.requester_name,
                old.calendar_week,
                old.customer,
                old.planned_hours
              ]
            );
          }
        }
      }
    }

    if (!dryRun) await client.query("COMMIT");

    // update run history
    if (runId) {
      await pool.query(
        `
        UPDATE import_runs
        SET finished_at=NOW(),
            status='ok',
            target_end=COALESCE($2::date, target_end),
            date_from=$3::date,
            date_to=$4::date,
            date_cols=$5,
            imported=$6,
            inserted_rows=$7,
            updated_rows=$8,
            skipped_no_employee_rows=$9,
            note=$10
        WHERE run_id=$1
        `,
        [
          runId,
          targetEndIso,
          dates[0].iso,
          dates[dates.length - 1].iso,
          dates.length,
          imported,
          inserted_rows,
          updated_rows,
          skippedNoEmployee,
          dryRun ? "dry-run ok" : "write ok"
        ]
      );
    }

    return {
      ok: true,
      run_id: runId,
      mode: dryRun ? "dry_run" : "write",
      imported: dryRun ? 0 : imported,
      inserted_rows: dryRun ? 0 : inserted_rows,
      updated_rows: dryRun ? 0 : updated_rows,
      would_write_rows: dryRun ? would_write_rows : 0,
      would_insert_rows: dryRun ? would_insert_rows : 0,
      would_update_rows: dryRun ? would_update_rows : 0,
      skipped_no_employee_rows: skippedNoEmployee,
      header_row: headerRow + 1,
      date_from: dates[0].iso,
      date_to: dates[dates.length - 1].iso,
      date_cols: dates.length,
      target_end: targetEndIso,
      optimization: {
        empty_streak_break: EMPTY_STREAK_BREAK,
        last_processed_excel_row_1based: lastProcessedRow ? lastProcessedRow + 1 : null,
      },
      note: dryRun
        ? "Dry-run: keine DB-Änderung."
        : "Write: inserted_rows/updated_rows via xmax; Rollback möglich über run_id.",
      duration_ms: Date.now() - startedAt,
      file_sha256: fileHash,
    };
  } catch (e) {
    if (!dryRun) {
      try { await client.query("ROLLBACK"); } catch {}
    }
    console.error("STAFFPLAN IMPORT ERROR:", e);

    if (runId) {
      try {
        await pool.query(
          `UPDATE import_runs SET finished_at=NOW(), status='failed', note=$2 WHERE run_id=$1`,
          [runId, `failed: ${e.message}`]
        );
      } catch {}
    }

    return { ok: false, error: e.message, run_id: runId };
  } finally {
    client.release();
  }
}

// ======================================================
// STAFFPLAN IMPORT (Upload)
// POST /api/import/staffplan?reset=1
// POST /api/import/staffplan?dry_run=1
// Optional: &target_end=YYYY-MM-DD
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    const dryRun = String(req.query.dry_run || "") === "1";
    const reset = String(req.query.reset || "0") === "1";
    const targetEndIso = String(req.query.target_end || "").trim() || null;

    const actorIp =
      (req.headers["x-forwarded-for"] ? String(req.headers["x-forwarded-for"]).split(",")[0].trim() : null) ||
      req.socket?.remoteAddress ||
      null;

    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const result = await doImportStaffplan({
      buffer: req.file.buffer,
      originalname: req.file.originalname || "upload.xlsx",
      dryRun,
      reset,
      targetEndIso,
      actorIp,
    });

    if (!result.ok) return res.status(500).json(result);
    return res.json(result);
  } catch (e) {
    console.error("UPLOAD IMPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// STAFFPLAN IMPORT (SharePoint / OneDrive link)
// POST /api/import/staffplan/sharepoint?reset=0
// POST /api/import/staffplan/sharepoint?dry_run=1
// Optional: &target_end=YYYY-MM-DD
// ======================================================
app.post("/api/import/staffplan/sharepoint", async (req, res) => {
  try {
    const dryRun = String(req.query.dry_run || "") === "1";
    const reset = String(req.query.reset || "0") === "1";
    const targetEndIso = String(req.query.target_end || "").trim() || null;

    const actorIp =
      (req.headers["x-forwarded-for"]
        ? String(req.headers["x-forwarded-for"]).split(",")[0].trim()
        : null) ||
      req.socket?.remoteAddress ||
      null;

    const url = await getSetting("staffplan_sharelink");
    if (!url) return res.status(400).json({ ok: false, error: "Kein SharePoint-Link gespeichert" });

    const buf = await downloadExcelFromShareLink(url);
    if (!buf || !buf.length) return res.status(500).json({ ok: false, error: "Download leer/fehlgeschlagen" });

    const hash = sha256Hex(buf);
    const lastHash = await getSetting("staffplan_last_sha256");

    // Skip nur im WRITE mode UND nur wenn NICHT reset
    if (!dryRun && !reset && lastHash && lastHash === hash) {
      return res.json({
        ok: true,
        skipped: true,
        reason: "unchanged_file_hash",
        sha256: hash,
        note: "Datei unverändert → kein Import ausgeführt",
      });
    }

    const result = await doImportStaffplan({
      buffer: buf,
      originalname: "sharepoint.xlsx",
      dryRun,
      reset,
      targetEndIso,
      actorIp,
    });

    if (!result.ok) return res.status(500).json(result);

    // last import info nur im WRITE mode (dry-run speichert NICHT)
    if (!dryRun) {
      await setSetting("staffplan_last_sha256", hash);
      await setSetting("staffplan_last_run_id", String(result.run_id || ""));
      await setSetting("staffplan_last_import_at", new Date().toISOString());
    }

    return res.json({
      ...result,
      sharepoint: { url_saved: true, sha256: hash, skipped_due_to_hash: false },
    });
  } catch (e) {
    console.error("SHAREPOINT IMPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: STAFFPLAN + ABSENCES OVERLAY  (Phase 1B)
// GET /api/admin/staffplan/with-absences?from=YYYY-MM-DD&to=YYYY-MM-DD
// returns staffplan rows + absence_type (sick|vacation|null)
// ======================================================
app.get("/api/admin/staffplan/with-absences", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from fehlt oder ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to fehlt oder ungültig (YYYY-MM-DD)" });
    }
    if (to < from) {
      return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });
    }

    const r = await pool.query(
      `
      WITH abs AS (
        SELECT
          ea.employee_id,
          ea.type,
          ea.date_from,
          ea.date_to,
          ea.created_at
        FROM employee_absences ea
        WHERE ea.status = 'active'
          AND ea.date_to >= $1::date
          AND ea.date_from <= $2::date
      )
      SELECT
        s.*,
        a.absence_type,
        CASE
          WHEN a.absence_type = 'sick' THEN 0
          WHEN a.absence_type = 'vacation' THEN
            CASE
              WHEN EXTRACT(ISODOW FROM s.work_date) IN (6,7) THEN 0
              ELSE COALESCE(e.weekly_hours, 40) / 5.0
            END
          ELSE COALESCE(s.planned_hours, 0)
        END AS effective_planned_hours
      FROM staffplan s
      LEFT JOIN employees e
        ON e.employee_id = s.employee_id

      -- WICHTIG: verhindert Duplikate bei mehrfachen/überlappenden Abwesenheiten:
      LEFT JOIN LATERAL (
        SELECT
          x.type AS absence_type
        FROM abs x
        WHERE x.employee_id = s.employee_id
          AND s.work_date BETWEEN x.date_from AND x.date_to
        ORDER BY
          CASE WHEN x.type = 'sick' THEN 2 ELSE 1 END DESC,
          x.created_at DESC
        LIMIT 1
      ) a ON TRUE

      WHERE s.work_date BETWEEN $1::date AND $2::date
      ORDER BY s.work_date ASC, s.employee_name ASC, s.id ASC
      `,
      [from, to]
    );

    return res.json({ ok: true, from, to, rows: r.rows });
  } catch (e) {
    console.error("WITH-ABSENCES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: STAFFPLAN EDIT (planned_hours)
// PATCH /api/admin/staffplan/planned-hours
// Body: { employee_id, work_date, customer_po, internal_po, project_short, planned_hours }
// ======================================================
app.patch("/api/admin/staffplan/planned-hours", async (req, res) => {
  try {
    const employee_id = String(req.body?.employee_id || "").trim();
    const work_date = String(req.body?.work_date || "").trim();

    // key parts (can be null/empty)
    const customer_po = req.body?.customer_po != null ? String(req.body.customer_po).trim() : null;
    const internal_po = req.body?.internal_po != null ? String(req.body.internal_po).trim() : null;
    const project_short = req.body?.project_short != null ? String(req.body.project_short).trim() : null;

    const planned_hours_raw = req.body?.planned_hours;

    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(work_date)) {
      return res.status(400).json({ ok: false, error: "work_date ungültig (YYYY-MM-DD)" });
    }

    let planned_hours = null;
    if (planned_hours_raw !== null && planned_hours_raw !== undefined && String(planned_hours_raw).trim() !== "") {
      const n = Number(planned_hours_raw);
      if (!isFinite(n) || n < 0) {
        return res.status(400).json({ ok: false, error: "planned_hours ungültig" });
      }
      planned_hours = n;
    }

    const r = await pool.query(
      `
      UPDATE staffplan
      SET planned_hours = $1
      WHERE employee_id = $2
        AND work_date = $3::date
        AND COALESCE(customer_po,'') = COALESCE($4,'')
        AND COALESCE(internal_po,'') = COALESCE($5,'')
        AND COALESCE(project_short,'') = COALESCE($6,'')
      `,
      [planned_hours, employee_id, work_date, customer_po, internal_po, project_short]
    );

    if (!r.rowCount) {
      return res.status(404).json({
        ok: false,
        error: "Staffplan-Zeile nicht gefunden (Key passt nicht)."
      });
    }

    return res.json({ ok: true, updated: r.rowCount });
  } catch (e) {
    console.error("STAFFPLAN PLANNED-HOURS PATCH ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});
// ======================================================
// ADMIN: Report Hours (Phase 2A) - uses clamped_hours
// ======================================================
// GET /api/admin/report-hours?from=YYYY-MM-DD&to=YYYY-MM-DD&employee_id=&customer_po=
app.get("/api/admin/report-hours", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const employee_id = req.query.employee_id ? String(req.query.employee_id).trim() : null;
    const customer_po = req.query.customer_po ? String(req.query.customer_po).trim() : null;

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    }
    if (to < from) {
      return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });
    }

    const where = [];
    const params = [from, to];

    where.push(`work_date BETWEEN $1::date AND $2::date`);
    where.push(`start_ts IS NOT NULL`);
    where.push(`end_ts IS NOT NULL`);
    where.push(`clamped_hours IS NOT NULL`);

    if (employee_id) {
      params.push(employee_id);
      where.push(`employee_id = $${params.length}`);
    }
if (customer_po) {
  params.push(customer_po);
  where += ` AND regexp_replace(COALESCE(sp.customer_po, p.customer_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}

    const r = await pool.query(
      `
      SELECT
        work_date,
        employee_id,
        mapped_customer_po,
        COUNT(*)::int AS entries,
        ROUND(SUM(clamped_hours)::numeric, 4) AS hours
      FROM v_time_entries_clamped
      WHERE ${where.join(" AND ")}
      GROUP BY work_date, employee_id, mapped_customer_po
      ORDER BY work_date ASC, employee_id ASC, mapped_customer_po ASC
      `,
      params
    );

    return res.json({ ok: true, from, to, rows: r.rows });
  } catch (e) {
    console.error("REPORT HOURS ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Report Hours Summary (Phase 2A)
// ======================================================
// GET /api/admin/report-hours/summary?from=YYYY-MM-DD&to=YYYY-MM-DD&include_po=0|1
app.get("/api/admin/report-hours/summary", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const include_po = String(req.query.include_po || "0") === "1";

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    }
    if (to < from) {
      return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });
    }

    const groupCols = include_po
      ? `employee_id, mapped_customer_po, mapped_internal_po`
      : `employee_id`;

    const selectCols = include_po
      ? `employee_id, mapped_customer_po, mapped_internal_po`
      : `employee_id`;

    const orderBy = include_po
      ? `ORDER BY employee_id ASC, mapped_customer_po ASC, mapped_internal_po ASC`
      : `ORDER BY employee_id ASC`;

    const r = await pool.query(
      `
      SELECT
        ${selectCols},
        COUNT(DISTINCT work_date)::int AS days,
        ROUND(SUM(clamped_hours)::numeric, 4) AS hours
      FROM v_time_entries_clamped
      WHERE work_date BETWEEN $1::date AND $2::date
        AND clamped_hours IS NOT NULL
      GROUP BY ${groupCols}
      ${orderBy}
      `,
      [from, to]
    );

    return res.json({ ok: true, from, to, include_po, rows: r.rows });
  } catch (e) {
    console.error("REPORT HOURS SUMMARY ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Report Hours Weekly (KW) - JSON
// GET /api/admin/report-hours/weekly?from=YYYY-MM-DD&to=YYYY-MM-DD
// Optional: &customer_po=&internal_po=&employee_id=
// ======================================================
app.get("/api/admin/report-hours/weekly", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const employee_id = req.query.employee_id ? String(req.query.employee_id).trim() : null;
    const customer_po = req.query.customer_po ? String(req.query.customer_po).trim() : null;
    const internal_po = req.query.internal_po != null ? String(req.query.internal_po).trim() : null; // kann "" sein

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    if (to < from) return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });

    const where = [];
    const params = [from, to];

    where.push(`work_date BETWEEN $1::date AND $2::date`);
    where.push(`clamped_hours IS NOT NULL`);

    if (employee_id) {
      params.push(employee_id);
      where.push(`employee_id = $${params.length}`);
    }
if (customer_po) {
  params.push(customer_po);
  where += ` AND regexp_replace(COALESCE(sp.customer_po, p.customer_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}
    if (internal_po !== null) {
      params.push(internal_po);
      where.push(`COALESCE(mapped_internal_po,'') = $${params.length}`);
    }

    const r = await pool.query(
      `
      SELECT
        EXTRACT(ISOYEAR FROM work_date)::int AS isoyear,
        EXTRACT(WEEK FROM work_date)::int AS isoweek,
        employee_id,
        COALESCE(mapped_customer_po,'') AS customer_po,
        COALESCE(mapped_internal_po,'') AS internal_po,
        COUNT(DISTINCT work_date)::int AS days,
        ROUND(SUM(clamped_hours)::numeric, 2) AS hours
      FROM v_time_entries_clamped
      WHERE ${where.join(" AND ")}
      GROUP BY
        EXTRACT(ISOYEAR FROM work_date)::int,
        EXTRACT(WEEK FROM work_date)::int,
        employee_id,
        COALESCE(mapped_customer_po,''),
        COALESCE(mapped_internal_po,'')
      ORDER BY isoyear ASC, isoweek ASC, employee_id ASC, customer_po ASC, internal_po ASC
      `,
      params
    );

    return res.json({ ok: true, from, to, rows: r.rows });
  } catch (e) {
    console.error("WEEKLY REPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/staffplan-topdates", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT work_date, COUNT(*)::int AS cnt
      FROM staffplan
      GROUP BY work_date
      ORDER BY work_date DESC
      LIMIT 15
    `);
    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("DEBUG STAFFPLAN TOPDATES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/debug/staffplan-check", async (req, res) => {
  try {
    const date = String(req.query.date || "").trim();
    if (!date) return res.status(400).json({ ok: false, error: "date fehlt (YYYY-MM-DD)" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) return res.status(400).json({ ok: false, error: "date ungültig (YYYY-MM-DD)" });

    const totalOnDate = await pool.query(
      `SELECT COUNT(*)::int AS cnt FROM staffplan WHERE work_date = $1::date`,
      [date]
    );
    return res.json({
      ok: true,
      date,
      total_on_date: totalOnDate.rows[0].cnt
    });
  } catch (e) {
    console.error("DEBUG STAFFPLAN CHECK ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Mitarbeiter-IDs (Hybrid AUTO_* -> echte ID)
// ======================================================
function isAutoEmployeeId(id) {
  return String(id || "").startsWith("AUTO_");
}

// (Optional) einfacher Employees-GET Endpunkt
app.get("/api/employees", async (req, res) => {
  try {
    const r = await pool.query(`SELECT employee_id, name, email, language FROM employees ORDER BY name`);
    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("EMPLOYEES GET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Admin: Übersicht + Usage Counts + Absent-Status (heute)
app.get("/api/admin/employees", async (req, res) => {
  try {
    const today = todayIsoBerlin();

    const r = await pool.query(`
      SELECT
        e.employee_id,
        e.name,
        e.email,
        e.language,
        e.weekly_hours,
        CASE WHEN e.employee_id LIKE 'AUTO\\_%' THEN 'auto' ELSE 'manual' END AS id_source,
        COALESCE(sp.cnt, 0)::int AS staffplan_rows,
        COALESCE(te.cnt, 0)::int AS time_rows,
        COALESCE(br.cnt, 0)::int AS break_rows,
        EXISTS (
          SELECT 1
          FROM employee_absences a
          WHERE a.employee_id = e.employee_id
            AND a.status = 'active'
            AND $1::date BETWEEN a.date_from AND a.date_to
        ) AS is_absent_today
      FROM employees e
      LEFT JOIN (SELECT employee_id, COUNT(*) AS cnt FROM staffplan GROUP BY employee_id) sp
        ON sp.employee_id = e.employee_id
      LEFT JOIN (SELECT employee_id, COUNT(*) AS cnt FROM time_entries GROUP BY employee_id) te
        ON te.employee_id = e.employee_id
      LEFT JOIN (
        SELECT employee_id, COUNT(*) AS cnt
        FROM time_events
        WHERE event_type IN ('break_start','break_end')
        GROUP BY employee_id
      ) br
        ON br.employee_id = e.employee_id
      ORDER BY (CASE WHEN e.employee_id LIKE 'AUTO\\_%' THEN 1 ELSE 0 END) ASC, e.name ASC
    `, [today]);

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("ADMIN EMPLOYEES ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Admin: manuell anlegen
app.post("/api/admin/employees", async (req, res) => {
  try {
    const employee_id = String(req.body.employee_id || "").trim();
    const name = String(req.body.name || "").trim();
    const email = req.body.email ? String(req.body.email).trim() : null;
    const language = req.body.language ? String(req.body.language).trim() : "de";

    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!name) return res.status(400).json({ ok: false, error: "name fehlt" });
    if (isAutoEmployeeId(employee_id)) {
      return res.status(400).json({ ok: false, error: "employee_id darf nicht mit AUTO_ beginnen" });
    }

    await pool.query(
      `INSERT INTO employees (employee_id, name, email, language) VALUES ($1,$2,$3,$4)`,
      [employee_id, name, email, language]
    );

    return res.json({ ok: true });
  } catch (e) {
    if (String(e.code) === "23505") {
      return res.status(409).json({ ok: false, error: "employee_id existiert bereits" });
    }
    console.error("ADMIN CREATE EMPLOYEE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Admin: Daten ändern
app.patch("/api/admin/employees", async (req, res) => {
  try {
    const employee_id = String(req.body.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const name = req.body.name != null ? String(req.body.name).trim() : null;
    const email = req.body.email != null ? String(req.body.email).trim() : null;
    const language = req.body.language != null ? String(req.body.language).trim() : null;

    const weekly_hours =
      req.body.weekly_hours != null && String(req.body.weekly_hours).trim() !== ""
        ? Number(req.body.weekly_hours)
        : null;

    if (weekly_hours !== null && (!isFinite(weekly_hours) || weekly_hours <= 0 || weekly_hours > 80)) {
      return res.status(400).json({ ok: false, error: "weekly_hours ungültig" });
    }

    const exists = await pool.query(`SELECT employee_id FROM employees WHERE employee_id=$1`, [employee_id]);
    if (!exists.rowCount) return res.status(404).json({ ok: false, error: "employee_id nicht gefunden" });

    await pool.query(
      `
      UPDATE employees
      SET name = COALESCE($2, name),
          email = COALESCE($3, email),
          language = COALESCE($4, language),
          weekly_hours = COALESCE($5, weekly_hours)
      WHERE employee_id=$1
      `,
      [employee_id, name || null, email || null, language || null, weekly_hours]
    );

    return res.json({ ok: true });
  } catch (e) {
    console.error("ADMIN UPDATE EMPLOYEE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// Admin: ID umhängen (AUTO_* -> echte ID)
app.post("/api/admin/employee-id", async (req, res) => {
  const oldId = String(req.body.old_employee_id || "").trim();
  const newId = String(req.body.new_employee_id || "").trim();
  const merge = String(req.body.merge || "keep_new").trim(); // keep_new|keep_old

  if (!oldId) return res.status(400).json({ ok: false, error: "old_employee_id fehlt" });
  if (!newId) return res.status(400).json({ ok: false, error: "new_employee_id fehlt" });
  if (oldId === newId) return res.status(400).json({ ok: false, error: "old und new sind gleich" });
  if (isAutoEmployeeId(newId)) return res.status(400).json({ ok: false, error: "new_employee_id darf nicht mit AUTO_ beginnen" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const oldEmp = await client.query(
      `SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1 FOR UPDATE`,
      [oldId]
    );
    if (!oldEmp.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "old_employee_id nicht gefunden" });
    }

    const newEmp = await client.query(
      `SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1 FOR UPDATE`,
      [newId]
    );

    const sp = await client.query(`UPDATE staffplan SET employee_id=$1 WHERE employee_id=$2`, [newId, oldId]);
    const te = await client.query(`UPDATE time_entries SET employee_id=$1 WHERE employee_id=$2`, [newId, oldId]);

    // Breaks: primär time_events (passt zu deinem /api/admin/employees Count)
    let br_time_events = 0;
    try {
      const br = await client.query(`UPDATE time_events SET employee_id=$1 WHERE employee_id=$2`, [newId, oldId]);
      br_time_events = br.rowCount;
    } catch (e) {
      // falls time_events nicht existiert o.ä.
      br_time_events = 0;
    }

    // optional: falls es bei dir doch eine "breaks" Tabelle gibt
    let br_breaks_table = 0;
    try {
      const br2 = await client.query(`UPDATE breaks SET employee_id=$1 WHERE employee_id=$2`, [newId, oldId]);
      br_breaks_table = br2.rowCount;
    } catch (e) {
      br_breaks_table = 0;
    }

    if (!newEmp.rowCount) {
      await client.query(`UPDATE employees SET employee_id=$1 WHERE employee_id=$2`, [newId, oldId]);
    } else {
      if (merge === "keep_old") {
        await client.query(
          `
          UPDATE employees
          SET name=$2,
              email=COALESCE($3,email),
              language=COALESCE($4,language)
          WHERE employee_id=$1
          `,
          [newId, oldEmp.rows[0].name, oldEmp.rows[0].email, oldEmp.rows[0].language]
        );
      }
      await client.query(`DELETE FROM employees WHERE employee_id=$1`, [oldId]);
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,
      old_employee_id: oldId,
      new_employee_id: newId,
      updated: {
        staffplan: sp.rowCount,
        time_entries: te.rowCount,
        time_events: br_time_events,
        breaks: br_breaks_table
      },
      merge_mode: newEmp.rowCount ? merge : "rename_pk"
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("ADMIN EMPLOYEE-ID ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// Admin: löschen (nur wenn unbenutzt)
app.delete("/api/admin/employees", async (req, res) => {
  try {
    const employee_id = String(req.query.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    // staffplan + time_entries sind fix
    const staffplanCntQ = await pool.query(`SELECT COUNT(*)::int AS cnt FROM staffplan WHERE employee_id=$1`, [employee_id]);
    const timeCntQ = await pool.query(`SELECT COUNT(*)::int AS cnt FROM time_entries WHERE employee_id=$1`, [employee_id]);

    // breaks: primär time_events, optional breaks table
    let breakCnt = 0;
    try {
      const br = await pool.query(
        `SELECT COUNT(*)::int AS cnt FROM time_events WHERE employee_id=$1 AND event_type IN ('break_start','break_end')`,
        [employee_id]
      );
      breakCnt += br.rows[0].cnt || 0;
    } catch {}

    try {
      const br2 = await pool.query(`SELECT COUNT(*)::int AS cnt FROM breaks WHERE employee_id=$1`, [employee_id]);
      breakCnt += br2.rows[0].cnt || 0;
    } catch {}

    const usage = {
      staffplan_cnt: staffplanCntQ.rows[0].cnt,
      time_cnt: timeCntQ.rows[0].cnt,
      break_cnt: breakCnt
    };

    if ((usage.staffplan_cnt || 0) > 0 || (usage.time_cnt || 0) > 0 || (usage.break_cnt || 0) > 0) {
      return res.status(409).json({
        ok: false,
        error: "Mitarbeiter wird noch verwendet (staffplan/time/breaks). Erst umhängen oder Daten löschen.",
        usage
      });
    }

    const del = await pool.query(`DELETE FROM employees WHERE employee_id=$1`, [employee_id]);
    if (!del.rowCount) return res.status(404).json({ ok: false, error: "employee_id nicht gefunden" });

    return res.json({ ok: true });
  } catch (e) {
    console.error("ADMIN DELETE EMPLOYEE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: PO Work Rules (Phase 2A)
// ======================================================

// GET /api/admin/po-work-rules?customer_po=...
app.get("/api/admin/po-work-rules", async (req, res) => {
  try {
    const customer_po = req.query.customer_po != null ? String(req.query.customer_po).trim() : null;

    const r = customer_po
      ? await pool.query(
          `
          SELECT id, customer_po, weekday, start_time, grace_minutes, created_at, updated_at
          FROM po_work_rules
          WHERE customer_po = $1
          ORDER BY customer_po ASC, weekday ASC
          `,
          [customer_po]
        )
      : await pool.query(
          `
          SELECT id, customer_po, weekday, start_time, grace_minutes, created_at, updated_at
          FROM po_work_rules
          ORDER BY customer_po ASC, weekday ASC
          `
        );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("PO WORK RULES GET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /api/admin/po-work-rules
// Body: { customer_po, weekday (1..7), start_time ("07:00"), grace_minutes? }
app.post("/api/admin/po-work-rules", async (req, res) => {
  try {
    const customer_po = String(req.body?.customer_po || "").trim();
    const weekday = Number(req.body?.weekday);
    const start_time = String(req.body?.start_time || "").trim();
    const grace_minutes =
      req.body?.grace_minutes != null && String(req.body.grace_minutes).trim() !== ""
        ? Number(req.body.grace_minutes)
        : 0;

    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po fehlt" });
    if (!Number.isFinite(weekday) || weekday < 1 || weekday > 7) {
      return res.status(400).json({ ok: false, error: "weekday muss 1..7 sein (ISO: 1=Mo..7=So)" });
    }
    if (!/^\d{2}:\d{2}(:\d{2})?$/.test(start_time)) {
      return res.status(400).json({ ok: false, error: "start_time ungültig (HH:MM oder HH:MM:SS)" });
    }
    if (!Number.isFinite(grace_minutes) || grace_minutes < 0 || grace_minutes > 120) {
      return res.status(400).json({ ok: false, error: "grace_minutes ungültig (0..120)" });
    }

    const r = await pool.query(
      `
      INSERT INTO po_work_rules (customer_po, weekday, start_time, grace_minutes, created_at, updated_at)
      VALUES ($1, $2, $3::time, $4, NOW(), NOW())
      RETURNING id
      `,
      [customer_po, weekday, start_time, grace_minutes]
    );

    return res.json({ ok: true, id: r.rows[0].id });
  } catch (e) {
    if (String(e.code) === "23505") {
      return res.status(409).json({ ok: false, error: "Regel existiert bereits für customer_po + weekday" });
    }
    console.error("PO WORK RULES POST ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// PATCH /api/admin/po-work-rules/:id
app.patch("/api/admin/po-work-rules/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id fehlt" });

    const customer_po = req.body?.customer_po != null ? String(req.body.customer_po).trim() : null;
    const weekday = req.body?.weekday != null && String(req.body.weekday).trim() !== "" ? Number(req.body.weekday) : null;
    const start_time = req.body?.start_time != null ? String(req.body.start_time).trim() : null;
    const grace_minutes =
      req.body?.grace_minutes != null && String(req.body.grace_minutes).trim() !== ""
        ? Number(req.body.grace_minutes)
        : null;

    if (customer_po !== null && !customer_po) return res.status(400).json({ ok: false, error: "customer_po ungültig" });
    if (weekday !== null && (!Number.isFinite(weekday) || weekday < 1 || weekday > 7)) {
      return res.status(400).json({ ok: false, error: "weekday muss 1..7 sein" });
    }
    if (start_time !== null && !/^\d{2}:\d{2}(:\d{2})?$/.test(start_time)) {
      return res.status(400).json({ ok: false, error: "start_time ungültig (HH:MM oder HH:MM:SS)" });
    }
    if (grace_minutes !== null && (!Number.isFinite(grace_minutes) || grace_minutes < 0 || grace_minutes > 120)) {
      return res.status(400).json({ ok: false, error: "grace_minutes ungültig (0..120)" });
    }

    const r = await pool.query(
      `
      UPDATE po_work_rules
      SET customer_po = COALESCE($2, customer_po),
          weekday = COALESCE($3, weekday),
          start_time = COALESCE($4::time, start_time),
          grace_minutes = COALESCE($5, grace_minutes),
          updated_at = NOW()
      WHERE id = $1::bigint
      `,
      [id, customer_po, weekday, start_time, grace_minutes]
    );

    if (!r.rowCount) return res.status(404).json({ ok: false, error: "Regel nicht gefunden" });
    return res.json({ ok: true });
  } catch (e) {
    if (String(e.code) === "23505") {
      return res.status(409).json({ ok: false, error: "Konflikt: customer_po + weekday existiert bereits" });
    }
    console.error("PO WORK RULES PATCH ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// DELETE /api/admin/po-work-rules/:id
app.delete("/api/admin/po-work-rules/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id fehlt" });

    const r = await pool.query(`DELETE FROM po_work_rules WHERE id=$1::bigint`, [id]);
    if (!r.rowCount) return res.status(404).json({ ok: false, error: "Regel nicht gefunden" });

    return res.json({ ok: true });
  } catch (e) {
    console.error("PO WORK RULES DELETE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Clamp Preview (Phase 2A)
// ======================================================

// GET /api/admin/clamp-preview?from=YYYY-MM-DD&to=YYYY-MM-DD&employee_id=&customer_po=
app.get("/api/admin/clamp-preview", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const employee_id = req.query.employee_id ? String(req.query.employee_id).trim() : null;
    const customer_po = req.query.customer_po ? String(req.query.customer_po).trim() : null;

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    }
    if (to < from) {
      return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });
    }

    const where = [];
    const params = [from, to];
    where.push(`work_date BETWEEN $1::date AND $2::date`);

    // nur echte Einträge
    where.push(`start_ts IS NOT NULL`);
    where.push(`end_ts IS NOT NULL`);

    if (employee_id) {
      params.push(employee_id);
      where.push(`employee_id = $${params.length}`);
    }

    // filtert auf mapped_customer_po (aus staffplan), nicht te.customer_po
 if (customer_po) {
  params.push(customer_po);
  where += ` AND regexp_replace(COALESCE(sp.customer_po, p.customer_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}


    const r = await pool.query(
      `
      SELECT
        employee_id,
        work_date,
        mapped_customer_po,
        start_ts,
        allowed_start_ts,
        effective_start_ts,
        end_ts,
        break_minutes,
        auto_break_minutes,
        clamped_hours
      FROM v_time_entries_clamped
      WHERE ${where.join(" AND ")}
      ORDER BY work_date DESC, employee_id ASC
      LIMIT 500
      `,
      params
    );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("CLAMP PREVIEW ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: PO Work Rules (Phase 2A) – READ ONLY TEST
// HINWEIS: NICHT doppelt definieren. Wenn du schon CRUD-Endpunkte hast,
// dann lass NUR diesen READ-ONLY weg ODER ersetze den anderen.
// In dieser Version ist es ein Alias-Endpunkt, damit es nicht kollidiert.
// ======================================================
app.get("/api/admin/po-work-rules/read-only", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT
        id,
        customer_po,
        weekday,
        start_time,
        grace_minutes,
        created_at,
        updated_at
      FROM po_work_rules
      ORDER BY customer_po ASC, weekday ASC
    `);
    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("PO WORK RULES GET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Helper – list available customer_po values
// ======================================================
// GET /api/admin/customer-pos
app.get("/api/admin/customer-pos", async (req, res) => {
  try {
    const limit = Math.max(1, Math.min(500, Number(req.query.limit) || 200));

    const r = await pool.query(
      `
      SELECT
        TRIM(customer_po) AS customer_po,
        MAX(customer) AS customer,
        COUNT(*)::int AS cnt
      FROM staffplan
      WHERE customer_po IS NOT NULL
        AND TRIM(customer_po) <> ''
      GROUP BY TRIM(customer_po)
      ORDER BY cnt DESC
      LIMIT $1
      `,
      [limit]
    );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("CUSTOMER-PO LIST ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Helper – list internal_po values for a customer_po
// GET /api/admin/internal-pos?customer_po=...
// ======================================================
app.get("/api/admin/internal-pos", async (req, res) => {
  try {
    const customer_po = String(req.query.customer_po || "").trim();
    if (!customer_po) {
      return res.status(400).json({ ok: false, error: "customer_po fehlt" });
    }

    const r = await pool.query(
      `
      SELECT
        COALESCE(NULLIF(TRIM(internal_po),''), '') AS internal_po,
        COUNT(*)::int AS cnt
      FROM staffplan
      WHERE TRIM(customer_po) = $1
      GROUP BY COALESCE(NULLIF(TRIM(internal_po),''), '')
      ORDER BY cnt DESC, internal_po ASC
      `,
      [customer_po]
    );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("INTERNAL-PO LIST ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: ABSENCES API
// ======================================================

// GET /api/admin/absences?employee_id=...&status=active|all|cancelled
app.get("/api/admin/absences", async (req, res) => {
  try {
    const employee_id = String(req.query.employee_id || "").trim();
    const status = String(req.query.status || "active").trim(); // active|all|cancelled

    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    let where = `WHERE employee_id=$1`;
    const params = [employee_id];

    if (status === "active") where += ` AND status='active'`;
    else if (status === "cancelled") where += ` AND status='cancelled'`;
    // all => keine Zusatzfilter

    const r = await pool.query(
      `
      SELECT id, employee_id, type, date_from, date_to, note, status, created_at, updated_at
      FROM employee_absences
      ${where}
      ORDER BY date_from DESC, id DESC
      `,
      params
    );

    return res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error("ADMIN ABSENCES GET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /api/admin/absences
app.post("/api/admin/absences", async (req, res) => {
  try {
    const employee_id = String(req.body.employee_id || "").trim();
    const type = String(req.body.type || "sick").trim(); // sick|vacation
    const date_from = String(req.body.date_from || "").trim();
    const date_to = String(req.body.date_to || "").trim();
    const note = req.body.note != null ? String(req.body.note).trim() : null;

    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!["sick", "vacation"].includes(type)) return res.status(400).json({ ok: false, error: "type ungültig" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date_from)) return res.status(400).json({ ok: false, error: "date_from ungültig" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date_to)) return res.status(400).json({ ok: false, error: "date_to ungültig" });

    const df = new Date(date_from + "T00:00:00.000Z");
    const dt = new Date(date_to + "T00:00:00.000Z");
    if (dt.getTime() < df.getTime()) {
      return res.status(400).json({ ok: false, error: "date_to muss >= date_from sein" });
    }

    const emp = await pool.query(`SELECT employee_id FROM employees WHERE employee_id=$1`, [employee_id]);
    if (!emp.rowCount) return res.status(404).json({ ok: false, error: "employee_id nicht gefunden" });

    const ins = await pool.query(
      `
      INSERT INTO employee_absences (employee_id, type, date_from, date_to, note, status, created_at, updated_at)
      VALUES ($1,$2,$3::date,$4::date,$5,'active',NOW(),NOW())
      RETURNING id
      `,
      [employee_id, type, date_from, date_to, note]
    );

    return res.json({ ok: true, id: ins.rows[0].id });
  } catch (e) {
    console.error("ADMIN ABSENCES POST ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// PATCH /api/admin/absences/:id  body: { status: "cancelled"|"active" }
app.patch("/api/admin/absences/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    const status = String(req.body.status || "").trim(); // active|cancelled
    if (!id) return res.status(400).json({ ok: false, error: "id fehlt" });
    if (!["active", "cancelled"].includes(status)) {
      return res.status(400).json({ ok: false, error: "status ungültig" });
    }

    const upd = await pool.query(
      `
      UPDATE employee_absences
      SET status=$2,
          updated_at=NOW()
      WHERE id=$1::bigint
      `,
      [id, status]
    );
    if (!upd.rowCount) return res.status(404).json({ ok: false, error: "id nicht gefunden" });

    return res.json({ ok: true });
  } catch (e) {
    console.error("ADMIN ABSENCES PATCH ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// DELETE /api/admin/absences/:id
app.delete("/api/admin/absences/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!id) return res.status(400).json({ ok: false, error: "id fehlt" });

    const del = await pool.query(`DELETE FROM employee_absences WHERE id=$1::bigint`, [id]);
    if (!del.rowCount) return res.status(404).json({ ok: false, error: "id nicht gefunden" });

    return res.json({ ok: true });
  } catch (e) {
    console.error("ADMIN ABSENCES DELETE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: RESET IMPORT DATA (SAFE RESET)
// POST /api/admin/reset
// Body: { confirm: "RESET-ALL-IMPORT-DATA" }
// Löscht: staffplan, import history, AUTO_* employees
// ======================================================
app.post("/api/admin/reset", async (req, res) => {
  const confirm = String(req.body?.confirm || "").trim();
  if (confirm !== "RESET-ALL-IMPORT-DATA") {
    return res.status(400).json({
      ok: false,
      error: 'Bestätigung fehlt. Sende body.confirm="RESET-ALL-IMPORT-DATA".',
    });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Reihenfolge wichtig wegen FK staffplan_changes -> import_runs
    await client.query(`TRUNCATE staffplan RESTART IDENTITY`);
    await client.query(`TRUNCATE staffplan_changes RESTART IDENTITY`);
    await client.query(`TRUNCATE import_runs RESTART IDENTITY`);

    const delAuto = await client.query(`DELETE FROM employees WHERE employee_id LIKE 'AUTO_%'`);

    await client.query("COMMIT");

    return res.json({
      ok: true,
      note: "Reset durchgeführt: staffplan + import_history geleert, AUTO_* employees gelöscht.",
      deleted_auto_employees: delAuto.rowCount,
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("ADMIN RESET ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// ======================================================
// MANUAL STAFFPLAN DOWNLOAD (SharePoint) - protected by code=2012
// GET /api/staffplan/download?code=2012
// ======================================================
app.get("/api/staffplan/download", async (req, res) => {
  const code = String(req.query.code || "").trim();
  if (code !== "2012") {
    return res.status(403).json({ ok: false, error: "Code falsch oder fehlt (code=2012)" });
  }

  // Konsistent mit restlichem Code:
  const url = await getSetting("staffplan_sharelink");

  if (!url) {
    return res.status(400).json({
      ok: false,
      error: "Keine SharePoint-URL gesetzt (settings staffplan_sharelink).",
    });
  }

  try {
    const dl = await downloadExcelFromShareLink(url);

    let buffer, filename;
    if (Buffer.isBuffer(dl)) {
      buffer = dl;
      filename = `staffplan_${new Date().toISOString().slice(0, 10)}.xlsx`;
    } else {
      buffer = dl?.buffer;
      filename = dl?.filename || `staffplan_${new Date().toISOString().slice(0, 10)}.xlsx`;
    }

    if (!buffer || !Buffer.isBuffer(buffer)) {
      return res.status(500).json({ ok: false, error: "Download ok, aber kein Buffer erhalten." });
    }

    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    return res.end(buffer);
  } catch (e) {
    console.error("STAFFPLAN DOWNLOAD ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: STAFFPLAN UPLOAD (Code 2012 geschützt durch Guard)
// POST /api/admin/staffplan/upload
// ======================================================
app.post("/api/admin/staffplan/upload", upload.single("file"), async (req, res) => {
  try {
    const dryRun = String(req.query.dry_run || "") === "1";
    const reset = String(req.query.reset || "0") === "1";
    const targetEndIso = String(req.query.target_end || "").trim() || null;

    const actorIp =
      (req.headers["x-forwarded-for"] ? String(req.headers["x-forwarded-for"]).split(",")[0].trim() : null) ||
      req.socket?.remoteAddress ||
      null;

    if (!req.file) {
      return res.status(400).json({ ok: false, error: "Keine Datei hochgeladen" });
    }

    const result = await doImportStaffplan({
      buffer: req.file.buffer,
      originalname: req.file.originalname || "staffplan.xlsx",
      dryRun,
      reset,
      targetEndIso,
      actorIp,
    });

    if (!result.ok) {
      return res.status(500).json(result);
    }

    return res.json({
      ok: true,
      message: "Staffplan erfolgreich importiert",
      ...result
    });
  } catch (e) {
    console.error("ADMIN STAFFPLAN UPLOAD ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message || "Upload fehlgeschlagen" });
  }
});

// =============================
// ZEITERFASSUNG – STEMPLEN (A)
// =============================

// recompute day summary from time_events -> time_entries
async function recomputeTimeEntryForDay(employee_id, work_date_iso) {
  await pool.query(
    `
    WITH day_events AS (
      SELECT
        employee_id,
        (event_time AT TIME ZONE 'Europe/Berlin')::date AS work_date,
        event_type,
        event_time
      FROM time_events
      WHERE employee_id = $1
        AND (event_time AT TIME ZONE 'Europe/Berlin')::date = $2::date
    ),
    paired AS (
      SELECT
        employee_id,
        work_date,
        event_type,
        event_time,
        LEAD(event_type) OVER (PARTITION BY employee_id, work_date ORDER BY event_time) AS next_type,
        LEAD(event_time) OVER (PARTITION BY employee_id, work_date ORDER BY event_time) AS next_time
      FROM day_events
    ),
    agg AS (
      SELECT
        employee_id,
        work_date,
        MIN(event_time) FILTER (WHERE event_type='clock_in') AS start_ts,
        MAX(event_time) FILTER (WHERE event_type='clock_out') AS end_ts,
        COALESCE(
          CEIL(
            SUM(EXTRACT(EPOCH FROM (next_time - event_time)) / 60.0)
              FILTER (WHERE event_type='break_start' AND next_type='break_end')
          ),
          0
        )::int AS break_minutes
      FROM paired
      GROUP BY employee_id, work_date
    )
    INSERT INTO time_entries (employee_id, work_date, start_ts, end_ts, break_minutes, auto_break_minutes)
    SELECT employee_id, work_date, start_ts, end_ts, break_minutes, 0
    FROM agg
    ON CONFLICT (employee_id, work_date) DO UPDATE
    SET start_ts = EXCLUDED.start_ts,
        end_ts = EXCLUDED.end_ts,
        break_minutes = EXCLUDED.break_minutes,
        auto_break_minutes = 0;
    `,
    [employee_id, work_date_iso]
  );
}

// POST /api/clock/in
app.post("/api/clock/in", async (req, res) => {
  try {
    const employee_id = String(req.body?.employee_id || "").trim();
    const project_id = req.body?.project_id != null ? String(req.body.project_id).trim() : null;
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    await ensureEmployeeExists(employee_id);

    const work_date = todayIsoBerlin();

    await pool.query(
      `INSERT INTO time_events (employee_id, project_id, event_type) VALUES ($1,$2,'clock_in')`,
      [employee_id, project_id]
    );

    await recomputeTimeEntryForDay(employee_id, work_date);
    return res.json({ ok: true, employee_id, work_date });
  } catch (e) {
    console.error("CLOCK IN ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /api/clock/out
app.post("/api/clock/out", async (req, res) => {
  try {
    const employee_id = String(req.body?.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    await ensureEmployeeExists(employee_id);

    const work_date = todayIsoBerlin();

    await pool.query(
      `INSERT INTO time_events (employee_id, event_type) VALUES ($1,'clock_out')`,
      [employee_id]
    );

    await recomputeTimeEntryForDay(employee_id, work_date);
    return res.json({ ok: true, employee_id, work_date });
  } catch (e) {
    console.error("CLOCK OUT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// DEBUG: proof that THIS server.js is running
app.get("/api/debug/has-break-routes", (req, res) => {
  res.json({ ok: true, hasBreakRoutes: true });
});

// POST /api/break/start
app.post("/api/break/start", async (req, res) => {
  try {
    const employee_id = String(req.body?.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    await ensureEmployeeExists(employee_id);

    const work_date = todayIsoBerlin();

    await pool.query(
      `INSERT INTO time_events (employee_id, event_type) VALUES ($1,'break_start')`,
      [employee_id]
    );

    await recomputeTimeEntryForDay(employee_id, work_date);
    return res.json({ ok: true, employee_id, work_date });
  } catch (e) {
    console.error("BREAK START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// POST /api/break/end
app.post("/api/break/end", async (req, res) => {
  try {
    const employee_id = String(req.body?.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    await ensureEmployeeExists(employee_id);

    const work_date = todayIsoBerlin();

    await pool.query(
      `INSERT INTO time_events (employee_id, event_type) VALUES ($1,'break_end')`,
      [employee_id]
    );

    await recomputeTimeEntryForDay(employee_id, work_date);
    return res.json({ ok: true, employee_id, work_date });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// TERMINAL: Login + Allowed Projects
// ======================================================
// GET /api/terminal/login?employee_id=...
// GET /api/terminal/login?employee_id=...&date=YYYY-MM-DD
app.get("/api/terminal/login", async (req, res) => {
  try {
    const q = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || "").trim() || todayIsoBerlin();

    if (!q) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ ok: false, error: "date ungültig (YYYY-MM-DD)" });
    }

    const plannedExact = await pool.query(
      `
      SELECT employee_id, employee_name AS name
      FROM staffplan
      WHERE employee_id=$1 AND work_date=$2::date
      LIMIT 1
      `,
      [q, date]
    );
    if (plannedExact.rowCount) {
      return res.json({ ok: true, employee: plannedExact.rows[0] });
    }

    const plannedByName = await pool.query(
      `
      SELECT employee_id, employee_name AS name, COUNT(*)::int AS cnt
      FROM staffplan
      WHERE work_date=$2::date
        AND employee_name ILIKE $1
      GROUP BY employee_id, employee_name
      ORDER BY cnt DESC
      LIMIT 10
      `,
      ['%' + q + '%', date]
    );

    if (plannedByName.rowCount) {
      const best = plannedByName.rows[0];
      return res.json({
        ok: true,
        employee: { employee_id: best.employee_id, name: best.name },
        candidates: plannedByName.rows
      });
    }

    return res.json({
      ok: false,
      error: "Mitarbeiter ist an diesem Datum nicht eingeplant."
    });
  } catch (e) {
    console.error("TERMINAL LOGIN ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// GET /api/allowed-projects?employee_id=...&date=YYYY-MM-DD
app.get("/api/allowed-projects", async (req, res) => {
  try {
    const employee_id = String(req.query.employee_id || "").trim();
    const date = String(req.query.date || "").trim();

    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
      return res.status(400).json({ ok: false, error: "date ungültig (YYYY-MM-DD)" });
    }

    const r = await pool.query(
      `
      SELECT DISTINCT TRIM(project_short) AS project_id
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2::date
        AND COALESCE(TRIM(project_short),'') <> ''
      ORDER BY 1 ASC
      `,
      [employee_id, date]
    );

    const projects = r.rows.map(x => ({ project_id: x.project_id, name: x.project_id }));
    return res.json({ ok: true, projects });
  } catch (e) {
    console.error("ALLOWED PROJECTS ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// A8: INVOICES (Draft) - create invoice snapshot from existing clamped logic
// POST /api/admin/invoices/create
// Body: { customer_po, from, to, internal_po?, round_to?, round_mode?, min_day_hours?, cap_day_hours? }
// NOTE: amount/total_amount are in "hours" for now (no hourly rate yet).
// ======================================================
app.post("/api/admin/invoices/create", async (req, res) => {
  const client = await pool.connect();
  try {
    const customer_po = String(req.body?.customer_po || "").trim();
    const from = String(req.body?.from || "").trim();
    const to = String(req.body?.to || "").trim();
    const internal_po = req.body?.internal_po != null ? String(req.body.internal_po).trim() : null; // can be "" or null

    const round_to =
      req.body?.round_to != null && String(req.body.round_to).trim() !== ""
        ? Number(req.body.round_to)
        : null; // e.g. 0.25
    const round_mode = String(req.body?.round_mode || "nearest").trim(); // nearest|up|down
    const min_day_hours =
      req.body?.min_day_hours != null && String(req.body.min_day_hours).trim() !== ""
        ? Number(req.body.min_day_hours)
        : null;
    const cap_day_hours =
      req.body?.cap_day_hours != null && String(req.body.cap_day_hours).trim() !== ""
        ? Number(req.body.cap_day_hours)
        : null;

    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po fehlt" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    if (to < from) return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });

    if (round_to !== null && (!isFinite(round_to) || round_to <= 0 || round_to > 4)) {
      return res.status(400).json({ ok: false, error: "round_to ungültig (z.B. 0.25)" });
    }
    if (!["nearest", "up", "down"].includes(round_mode)) {
      return res.status(400).json({ ok: false, error: "round_mode ungültig (nearest|up|down)" });
    }
    for (const [k, v] of [["min_day_hours", min_day_hours], ["cap_day_hours", cap_day_hours]]) {
      if (v !== null && (!isFinite(v) || v < 0 || v > 24)) {
        return res.status(400).json({ ok: false, error: `${k} ungültig (0..24)` });
      }
    }

    function roundHours(val) {
      const x = Number(val);
      if (!isFinite(x)) return null;
      if (round_to === null) return x;

      const units = x / round_to;
      let r;
      if (round_mode === "up") r = Math.ceil(units);
      else if (round_mode === "down") r = Math.floor(units);
      else r = Math.round(units);

      return r * round_to;
    }

    function applyMinCap(h) {
      if (h === null) return null;
      let x = h;
      if (min_day_hours !== null) x = Math.max(x, min_day_hours);
      if (cap_day_hours !== null) x = Math.min(x, cap_day_hours);
      return x;
    }

    // bill_travel for this customer_po
    const btRow = await client.query(
      `SELECT bool_or(bill_travel) AS bill_travel FROM po_work_rules WHERE customer_po = $1`,
      [customer_po]
    );
    const bill_travel = !!btRow.rows?.[0]?.bill_travel;

    // pick customer label (best-effort)
    const custRow = await client.query(
      `
      SELECT MAX(customer) AS customer
      FROM staffplan
      WHERE customer_po = $1 AND customer IS NOT NULL AND customer <> ''
      `,
      [customer_po]
    );
    const customer = custRow.rows?.[0]?.customer || null;

    // daily base rows (same source as your CSV endpoints)
    const where = [];
    const params = [from, to, customer_po];

    where.push(`work_date BETWEEN $1::date AND $2::date`);
    where.push(`clamped_hours IS NOT NULL`);
    where.push(`mapped_customer_po = $3`);

    if (internal_po !== null) {
      params.push(internal_po);
      where.push(`COALESCE(mapped_internal_po,'') = $${params.length}`);
    }

    const daily = await client.query(
      `
      SELECT
        work_date,
        te.employee_id,
        COALESCE(e.name, te.employee_id) AS employee_name,
        COALESCE(te.mapped_internal_po,'') AS internal_po,
        SUM(te.clamped_hours)::numeric AS hours,
        SUM(COALESCE(te.travel_hours,0))::numeric AS travel_hours
      FROM v_time_entries_clamped te
      LEFT JOIN employees e ON e.employee_id = te.employee_id
      WHERE ${where.join(" AND ")}
      GROUP BY work_date, te.employee_id, employee_name, COALESCE(te.mapped_internal_po,'')
      ORDER BY work_date ASC, te.employee_id ASC, internal_po ASC
      `,
      params
    );

    // bucket per employee + internal_po; rounding is applied DAILY (like your summary logic)
    const bucket = new Map();

    for (const r of (daily.rows || [])) {
      const day = String(r.work_date).slice(0, 10);
      const rawHours = Number(r.hours) || 0;

      const rounded = roundHours(rawHours);
      const billedHours = applyMinCap(rounded);
      if (billedHours === null) continue;

      const travelRaw = Number(r.travel_hours) || 0;
      const travelBilled = bill_travel ? travelRaw : 0;

      const key = `${r.employee_id}||${r.internal_po}`;
      const prev = bucket.get(key) || {
        employee_id: r.employee_id,
        employee_name: r.employee_name,
        internal_po: r.internal_po,
        days: new Set(),
        hours_raw: 0,
        hours_billed: 0,
        travel_raw: 0,
        travel_billed: 0,
      };

      prev.days.add(day);
      prev.hours_raw += rawHours;
      prev.hours_billed += Number(billedHours) || 0;
      prev.travel_raw += travelRaw;
      prev.travel_billed += travelBilled;

      bucket.set(key, prev);
    }

    const lineItems = [];
    let totalHours = 0;

    for (const x of bucket.values()) {
      const h = Math.round((Number(x.hours_billed) || 0) * 100) / 100;
      const t = Math.round((Number(x.travel_billed) || 0) * 100) / 100;

      if (h > 0) {
        lineItems.push({
          description: `Arbeitszeit – ${x.employee_name} (${x.employee_id})${x.internal_po ? ` / Internal PO: ${x.internal_po}` : ""}`,
          quantity: h,
          unit: "h",
          unit_price: null,
          amount: h,
        });
        totalHours += h;
      }

      if (t > 0) {
        lineItems.push({
          description: `Reisezeit – ${x.employee_name} (${x.employee_id})${x.internal_po ? ` / Internal PO: ${x.internal_po}` : ""}`,
          quantity: t,
          unit: "h",
          unit_price: null,
          amount: t,
        });
        totalHours += t;
      }
    }

    if (!lineItems.length) {
      return res.status(400).json({
        ok: false,
        error: "Keine abrechenbaren Daten im Zeitraum (nach Regeln/Rundung).",
      });
    }

    await client.query("BEGIN");

    // create invoice (draft, no number)
    const totalRounded = Math.round(totalHours * 100) / 100;
    const inv = await client.query(
      `
      INSERT INTO invoices (customer_po, customer, period_start, period_end, status, currency, total_amount, source)
      VALUES ($1, $2, $3::date, $4::date, 'draft', 'EUR', $5, $6)
      RETURNING id
      `,
      [customer_po, customer, from, to, totalRounded, "clamped"]
    );
    const invoice_id = inv.rows[0].id;

    // insert lines
    for (const li of lineItems) {
      await client.query(
        `
        INSERT INTO invoice_lines (invoice_id, description, quantity, unit, unit_price, amount)
        VALUES ($1,$2,$3,$4,$5,$6)
        `,
        [invoice_id, li.description, li.quantity, li.unit, li.unit_price, li.amount]
      );
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,
      invoice_id,
      status: "draft",
      source: "clamped",
      customer_po,
      customer,
      period_start: from,
      period_end: to,
      currency: "EUR",
      total_amount: totalRounded,
      lines: lineItems.length,
      note: "total_amount/amount sind aktuell Stunden (noch kein Stundensatz hinterlegt).",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("INVOICE CREATE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});


// ======================================================
// ADMIN: Open Sessions (start_ts gesetzt, end_ts fehlt)
// GET /api/admin/open-sessions?from=YYYY-MM-DD&to=YYYY-MM-DD
// ======================================================
app.get("/api/admin/open-sessions", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from fehlt/ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to fehlt/ungültig (YYYY-MM-DD)" });
    }

    const r = await pool.query(
      `
      SELECT
        employee_id,
        work_date,
        mapped_customer_po,
        mapped_internal_po,
        start_ts,
        end_ts,
        break_minutes
      FROM v_time_entries_clamped
      WHERE work_date BETWEEN $1::date AND $2::date
        AND start_ts IS NOT NULL
        AND end_ts IS NULL
      ORDER BY work_date DESC, employee_id ASC
      `,
      [from, to]
    );

    res.json({ ok: true, from, to, rows: r.rows });
  } catch (e) {
    console.error("OPEN SESSIONS ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Report Hours Daily (für Nachweise)
// GET /api/admin/report-hours/daily?from=YYYY-MM-DD&to=YYYY-MM-DD&customer_po=&internal_po=
// ======================================================
app.get("/api/admin/report-hours/daily", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const customer_po = req.query.customer_po != null ? String(req.query.customer_po).trim() : "";
    const internal_po = req.query.internal_po != null ? String(req.query.internal_po).trim() : "";

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) {
      return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    }

    const params = [from, to];
    const where = [
      `work_date BETWEEN $1::date AND $2::date`,
      `clamped_hours IS NOT NULL`,
    ];

if (customer_po) {
  params.push(customer_po);
  where += ` AND regexp_replace(COALESCE(sp.customer_po, p.customer_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}

if (internal_po) {
  params.push(internal_po);
  where += ` AND regexp_replace(COALESCE(sp.internal_po, p.internal_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}


    const r = await pool.query(
      `
      SELECT
        work_date,
        employee_id,
        mapped_customer_po,
        mapped_internal_po,
        start_ts,
        end_ts,
        break_minutes,
        ROUND(clamped_hours::numeric, 4) AS hours
      FROM v_time_entries_clamped
      WHERE ${where.join(" AND ")}
      ORDER BY work_date ASC, employee_id ASC
      `,
      params
    );

    res.json({ ok: true, from, to, rows: r.rows });
  } catch (e) {
    console.error("REPORT DAILY ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// ADMIN: Report Hours Daily CSV
// GET /api/admin/report-hours/daily.csv?from=YYYY-MM-DD&to=YYYY-MM-DD&customer_po=&internal_po=
// ======================================================
app.get("/api/admin/report-hours/daily.csv", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const customer_po = req.query.customer_po != null ? String(req.query.customer_po).trim() : "";
    const internal_po = req.query.internal_po != null ? String(req.query.internal_po).trim() : "";

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) return res.status(400).send("from ungültig");
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) return res.status(400).send("to ungültig");

    const params = [from, to];
    const where = [
      `work_date BETWEEN $1::date AND $2::date`,
      `clamped_hours IS NOT NULL`,
    ];

if (customer_po) {
  params.push(customer_po);
  where += ` AND regexp_replace(COALESCE(sp.customer_po, p.customer_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}

if (internal_po) {
  params.push(internal_po);
  where += ` AND regexp_replace(COALESCE(sp.internal_po, p.internal_po, ''), '\\s', '', 'g')
               = regexp_replace($${params.length}, '\\s', '', 'g')`;
}


    const q = await pool.query(
      `
      SELECT
        work_date,
        employee_id,
        mapped_customer_po,
        mapped_internal_po,
        start_ts,
        end_ts,
        break_minutes,
        ROUND(clamped_hours::numeric, 4) AS hours
      FROM v_time_entries_clamped
      WHERE ${where.join(" AND ")}
      ORDER BY work_date ASC, employee_id ASC
      `,
      params
    );

    function csvCell(v) {
      const s = (v === null || v === undefined) ? "" : String(v);
      if (/[;"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
      return s;
    }

    let csv = "\ufeff" + [
      ["work_date","employee_id","customer_po","internal_po","start_ts","end_ts","break_minutes","hours"].join(";"),
    ].join("\n");

    for (const r of q.rows) {
      csv += "\n" + [
        csvCell(r.work_date),
        csvCell(r.employee_id),
        csvCell(r.mapped_customer_po),
        csvCell(r.mapped_internal_po),
        csvCell(r.start_ts),
        csvCell(r.end_ts),
        csvCell(r.break_minutes),
        csvCell(r.hours),
      ].join(";");
    }

    const filename = `report_daily_${from}_to_${to}.csv`;
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (e) {
    console.error("DAILY CSV ERROR:", e);
    res.status(500).send(e.message || "csv error");
  }
});
// ======================================================
// A8: INVOICES (Draft) - create invoice snapshot from STAFFPLAN (planned hours)
// POST /api/admin/invoices/create-planned
// Body: { customer_po, from, to, internal_po? }
// NOTE: amount/total_amount are HOURS (planned), no rates yet.
// ======================================================
app.post("/api/admin/invoices/create-planned", async (req, res) => {
  const client = await pool.connect();
  try {
    const customer_po = String(req.body?.customer_po || "").trim();
    const from = String(req.body?.from || "").trim();
    const to = String(req.body?.to || "").trim();
    const internal_po = req.body?.internal_po != null ? String(req.body.internal_po).trim() : null; // can be "" or null

    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po fehlt" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(from)) return res.status(400).json({ ok: false, error: "from ungültig (YYYY-MM-DD)" });
    if (!/^\d{4}-\d{2}-\d{2}$/.test(to)) return res.status(400).json({ ok: false, error: "to ungültig (YYYY-MM-DD)" });
    if (to < from) return res.status(400).json({ ok: false, error: "to darf nicht vor from liegen" });

    await client.query("BEGIN");

    // best-effort customer label
    const custRow = await client.query(
      `
      SELECT MAX(customer) AS customer
      FROM staffplan
      WHERE customer_po = $1 AND customer IS NOT NULL AND customer <> ''
      `,
      [customer_po]
    );
    const customer = custRow.rows?.[0]?.customer || null;

    // aggregate planned hours per employee (+ internal_po bucket)
    const where = [];
    const params = [customer_po, from, to];
    where.push(`customer_po = $1`);
    where.push(`work_date BETWEEN $2::date AND $3::date`);
    where.push(`planned_hours IS NOT NULL`);
    where.push(`planned_hours > 0`);

    if (internal_po !== null) {
      params.push(internal_po);
      where.push(`COALESCE(internal_po,'') = $${params.length}`);
    }

    const agg = await client.query(
      `
      SELECT
        employee_id,
        MAX(employee_name) AS employee_name,
        COALESCE(internal_po,'') AS internal_po,
        COUNT(DISTINCT work_date)::int AS days,
        SUM(planned_hours)::numeric AS hours
      FROM staffplan
      WHERE ${where.join(" AND ")}
      GROUP BY employee_id, COALESCE(internal_po,'')
      ORDER BY employee_id ASC, internal_po ASC
      `,
      params
    );

    if (!agg.rowCount) {
      await client.query("ROLLBACK");
      return res.status(400).json({
        ok: false,
        error: "Keine Planstunden im Zeitraum (staffplan.planned_hours).",
      });
    }

    // create invoice (draft, no number)
    const totalHours = Math.round((agg.rows.reduce((s, r) => s + (Number(r.hours) || 0), 0)) * 100) / 100;

    const inv = await client.query(
      `
      INSERT INTO invoices (customer_po, customer, period_start, period_end, status, currency, total_amount, source)
      VALUES ($1, $2, $3::date, $4::date, 'draft', 'EUR', $5, $6)
      RETURNING id
      `,
      [customer_po, customer, from, to, totalHours, "staffplan.planned_hours"]
    );
    const invoice_id = inv.rows[0].id;

    // lines
    let lines = 0;
    for (const r of agg.rows) {
      const hours = Math.round((Number(r.hours) || 0) * 100) / 100;
      if (hours <= 0) continue;

      const desc =
        `Planstunden – ${r.employee_name || r.employee_id} (${r.employee_id})` +
        (r.internal_po ? ` / Internal PO: ${r.internal_po}` : "") +
        ` / Tage: ${r.days}`;

      await client.query(
        `
        INSERT INTO invoice_lines (invoice_id, description, quantity, unit, unit_price, amount)
        VALUES ($1,$2,$3,'h',NULL,$3)
        `,
        [invoice_id, desc, hours]
      );
      lines++;
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,
      invoice_id,
      status: "draft",
      source: "staffplan.planned_hours",
      customer_po,
      customer,
      period_start: from,
      period_end: to,
      currency: "EUR",
      total_amount: totalHours,
      lines,
      note: "Rechnung basiert auf Planstunden (nicht auf echten Zeiten). total_amount/amount sind Stunden.",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("INVOICE CREATE PLANNED ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});


// ======================================================
// ADMIN: Import Employees from Excel (name, employee_id)
// POST /api/admin/import/employees  (multipart form-data: file)
// protected by /api/admin guard (x-admin-code: 2012)
// ======================================================
app.post("/api/admin/import/employees", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const rows = XLSX.utils.sheet_to_json(ws, { defval: "" });

    let inserted = 0, updated = 0, skipped = 0;
    const seen = new Set();

    for (const r of rows) {
      // Excel kann Spaltennamen unterschiedlich groß schreiben
      const rawId = (r.employee_id ?? r.Employee_ID ?? r.EmployeeId ?? r.id ?? "").toString().trim();
      const rawName = (r.name ?? r.Name ?? r.employee_name ?? "").toString().trim();

      if (!rawId || !rawName) { skipped++; continue; }

      // IDs aus Excel kommen oft als "1001" oder "1001.0"
      const employee_id = rawId.replace(/\.0$/, "").trim();
      const name = commaSwapName(rawName).trim(); // "Nachname, Vorname" -> "Vorname Nachname"

      // sanity
      if (!employee_id || !name) { skipped++; continue; }
      if (employee_id.startsWith("AUTO_")) { skipped++; continue; }

      // Doppelte in der Datei überspringen (oder später überschreiben)
      const key = employee_id;
      if (seen.has(key)) { skipped++; continue; }
      seen.add(key);

      // Insert/Update
      const q = await pool.query(
        `
        INSERT INTO employees (employee_id, name)
        VALUES ($1, $2)
        ON CONFLICT (employee_id) DO UPDATE
          SET name = EXCLUDED.name
        RETURNING (xmax = 0) AS inserted
        `,
        [employee_id, name]
      );

      if (q.rows[0]?.inserted) inserted++;
      else updated++;
    }

    res.json({
      ok: true,
      file: req.file.originalname,
      rows_in_file: rows.length,
      inserted,
      updated,
      skipped,
      note: "Name wird auf 'Vorname Nachname' normalisiert (Komma-Swap).",
    });
  } catch (e) {
    console.error("EMPLOYEE IMPORT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});
// ======================================================
// A9.11: Review Queue - list recent draft invoices
// GET /api/admin/invoices/review?minutes=60
// optional: &status=draft|final|exported|all
// ======================================================
app.get("/api/admin/review/invoices", async (req, res) => {
  try {
    const minutes = Math.max(1, Math.min(10080, Number(req.query.minutes) || 60)); // up to 7 days
    const status = req.query.status ? String(req.query.status).trim() : "draft";

    const allowed = new Set(["draft", "final", "exported", "all"]);
    if (!allowed.has(status)) {
      return res.status(400).json({ ok: false, error: "status muss draft|final|exported|all sein" });
    }

    const where = [];
    const params = [minutes];

    where.push(`created_at >= NOW() - ($1::int * INTERVAL '1 minute')`);

    if (status !== "all") {
      params.push(status);
      where.push(`status = $${params.length}`);
    }

    const q = await pool.query(
      `
      SELECT
        id,
        invoice_number,
        customer_po,
        customer,
        period_start,
        period_end,
        status,
        currency,
        total_amount,
        source,
        created_at,
        finalized_at,
        exported_at,
        export_note
      FROM invoices
      WHERE ${where.join(" AND ")}
      ORDER BY created_at DESC, id DESC
      LIMIT 300
      `,
      params
    );

    res.json({ ok: true, minutes, status, rows: q.rows });
  } catch (e) {
    console.error("REVIEW LIST ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// A8: INVOICES - PDF export (Erfassungsbogen)
// GET /api/admin/invoices/:id.pdf?code=2012
// ======================================================
app.get("/api/admin/invoices/:id.pdf", async (req, res) => {
  try {
    try { requireCode2012(req); } catch { return res.status(403).send("Falscher Sicherheitscode"); }

    const id = String(req.params.id || "").trim();
    if (!/^\d+$/.test(id)) return res.status(400).send("id ungültig");

    const inv = await pool.query(`SELECT * FROM invoices WHERE id=$1::bigint`, [id]);
    if (!inv.rowCount) return res.status(404).send("Invoice nicht gefunden");
    const invoice = inv.rows[0];

    // PG DATE kann als JS Date kommen -> sicher ISO bauen
    const fromIso =
      invoice.period_start instanceof Date
        ? invoice.period_start.toISOString().slice(0, 10)
        : String(invoice.period_start).slice(0, 10);

    const toIso =
      invoice.period_end instanceof Date
        ? invoice.period_end.toISOString().slice(0, 10)
        : String(invoice.period_end).slice(0, 10);

    const linesQ = await pool.query(
      `
      SELECT description, quantity, unit, unit_price, amount
      FROM invoice_lines
      WHERE invoice_id=$1::bigint
      ORDER BY id ASC
      `,
      [id]
    );
    const lines = linesQ.rows || [];

    // Tagesdaten (echte Zeiten) aus v_time_entries_clamped
    const daily = await pool.query(
      `
      SELECT
        work_date,
        employee_id,
        COALESCE(mapped_internal_po,'') AS internal_po,
        ROUND(SUM(clamped_hours)::numeric, 2) AS hours,
        ROUND(SUM(COALESCE(travel_hours,0))::numeric, 2) AS travel_hours
      FROM v_time_entries_clamped
      WHERE work_date BETWEEN $1::date AND $2::date
        AND mapped_customer_po = $3
        AND clamped_hours IS NOT NULL
      GROUP BY work_date, employee_id, COALESCE(mapped_internal_po,'')
      ORDER BY work_date ASC, employee_id ASC, internal_po ASC
      `,
      [fromIso, toIso, invoice.customer_po]
    );

    // Filename
    const safePo = String(invoice.customer_po || "PO").replace(/[^0-9A-Za-z_\-]/g, "_");
    const filename = `Erfassungsbogen_${safePo}_${fromIso}_bis_${toIso}.pdf`;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);

    const doc = new PDFDocument({ size: "A4", margin: 50 });
    doc.pipe(res);

    // Helpers (doc-scope)
    const PAGE_BOTTOM = 760;

    function drawHeader() {
      if (fs.existsSync(LOGO_FILE)) {
        doc.image(LOGO_FILE, 50, 35, { width: 120 });
      }

      doc
        .font("Helvetica-Bold")
        .fontSize(18)
        .text("Erfassungsbogen für Rechnungserstellung", 50, 40, { align: "right" });

      doc
        .font("Helvetica")
        .fontSize(10)
        .fillColor("#444")
        .text("Dieses Dokument ist keine Rechnung. Es dient als Grundlage zur Rechnungserstellung.", { align: "right" });

      doc.fillColor("black");
      doc.y = 120;
    }

    function ensureSpace(minBottom = 80) {
      if (doc.y > (PAGE_BOTTOM - minBottom)) {
        doc.addPage();
        drawHeader();
      }
    }

    function hr() {
      const y = doc.y;
      doc
        .moveTo(50, y)
        .lineTo(545, y)
        .strokeColor("#e5e5e5")
        .lineWidth(1)
        .stroke();
      doc.strokeColor("black").lineWidth(1);
      doc.moveDown(0.6);
    }

    function fmt(v) {
      if (v === null || v === undefined || v === "") return "";
      const n = Number(v);
      if (!isFinite(n)) return String(v);
      return n.toFixed(2);
    }

    // ===== PDF CONTENT =====
    drawHeader();

    // Meta
    doc.font("Helvetica-Bold").fontSize(11).text("Daten", { underline: true });
    doc.moveDown(0.4);

    doc.font("Helvetica").fontSize(10);
    doc.text(`Kunde: ${invoice.customer || "-"}`);
    doc.text(`Kunden-PO: ${invoice.customer_po || "-"}`);
    doc.text(`Zeitraum: ${fromIso} – ${toIso}`);
    doc.text(`Quelle: ${invoice.source === "clamped" ? "Echte Zeiten (erfasst)" : "Planstunden"}`);
    doc.moveDown(0.6);
    hr();

    // Block 1: Positionen
    doc.font("Helvetica-Bold").fontSize(11).text("Positionen (kompakt)");
    doc.moveDown(0.5);

    const xDesc = 50;
    const xQty = 430;
    const xUnit = 470;
    const xAmt = 505;

    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Beschreibung", xDesc, doc.y);
    doc.text("Menge", xQty, doc.y, { width: 40, align: "right" });
    doc.text("Einheit", xUnit, doc.y, { width: 35, align: "right" });
    doc.text("Summe", xAmt, doc.y, { width: 40, align: "right" });
    doc.moveDown(0.4);
    hr();

    doc.font("Helvetica").fontSize(9);

    let totalLines = 0;
    for (const li of lines) {
      ensureSpace(120);

      const y = doc.y;
      const desc = String(li.description || "");
      doc.text(desc, xDesc, y, { width: 365 });

      doc.text(fmt(li.quantity), xQty, y, { width: 40, align: "right" });
      doc.text(String(li.unit || ""), xUnit, y, { width: 35, align: "right" });
      doc.text(fmt(li.amount), xAmt, y, { width: 40, align: "right" });

      const h = doc.heightOfString(desc, { width: 365 });
      doc.y = y + Math.max(14, h) + 6;

      totalLines += Number(li.amount || 0) || 0;
    }

    hr();
    doc.font("Helvetica-Bold").fontSize(10);
    doc.text(`Gesamtsumme (aus Positionen): ${fmt(totalLines)} h`, { align: "right" });

    doc.moveDown(0.8);

    // Block 2: Stundenübersicht
    ensureSpace(160);
    doc.font("Helvetica-Bold").fontSize(11).text("Stundenübersicht (nach Datum)");
    doc.moveDown(0.5);

    const xDate = 50;
    const xEmp = 120;
    const xIPO = 260;
    const xH = 470;
    const xT = 520;

    doc.font("Helvetica-Bold").fontSize(9);
    doc.text("Datum", xDate, doc.y);
    doc.text("Mitarbeiter", xEmp, doc.y);
    doc.text("Internal PO", xIPO, doc.y);
    doc.text("Std", xH, doc.y, { width: 40, align: "right" });
    doc.text("Reise", xT, doc.y, { width: 40, align: "right" });
    doc.moveDown(0.4);
    hr();

    doc.font("Helvetica").fontSize(9);

    let sumHours = 0;
    let sumTravel = 0;

    for (const r of (daily.rows || [])) {
      ensureSpace(140);

      const date =
        r.work_date instanceof Date
          ? r.work_date.toISOString().slice(0, 10)
          : String(r.work_date).slice(0, 10);

      const emp = String(r.employee_id || "");
      const ipo = String(r.internal_po || "-");
      const h = Number(r.hours || 0) || 0;
      const t = Number(r.travel_hours || 0) || 0;

      const y = doc.y;
      doc.text(date, xDate, y, { width: 65 });
      doc.text(emp, xEmp, y, { width: 130 });
      doc.text(ipo || "-", xIPO, y, { width: 190 });
      doc.text(h.toFixed(2), xH, y, { width: 40, align: "right" });
      doc.text(t.toFixed(2), xT, y, { width: 40, align: "right" });

      doc.y = y + 16;
      sumHours += h;
      sumTravel += t;
    }

    hr();
    doc.font("Helvetica-Bold").fontSize(10);
    doc.text(`Summe Arbeitszeit: ${sumHours.toFixed(2)} h`, { align: "right" });
    doc.text(`Summe Reisezeit:  ${sumTravel.toFixed(2)} h`, { align: "right" });
    doc.text(`Gesamt:           ${(sumHours + sumTravel).toFixed(2)} h`, { align: "right" });

    doc.moveDown(0.8);
    doc.font("Helvetica").fontSize(8).fillColor("#555");
    doc.text("Hinweis: Dieser Erfassungsbogen ist keine Rechnung und dient ausschließlich als Grundlage zur Rechnungserstellung.");
    doc.fillColor("black");

    doc.end();
  } catch (e) {
    console.error("PDF ERROR:", e);
    if (String(req.query.debug || "") === "1") {
      return res.status(500).send("PDF Fehler: " + e.message);
    }
    return res.status(500).send("PDF Fehler");
  }
});

// ======================================================
// A8: INVOICES - CSV export from invoice_lines (semicolon, Excel-DE)
// GET /api/admin/invoices/:id.csv
// ======================================================
app.get("/api/admin/invoices/:id.csv", async (req, res) => {
  try {
    try { requireCode2012(req); } catch { return res.status(403).send("Falscher Sicherheitscode"); }

    const id = String(req.params.id || "").trim();
    if (!/^\d+$/.test(id)) return res.status(400).send("id ungültig");

    const inv = await pool.query(`SELECT * FROM invoices WHERE id=$1::bigint`, [id]);
    if (!inv.rowCount) return res.status(404).send("Invoice nicht gefunden");

    const lines = await pool.query(
      `SELECT description, quantity, unit, unit_price, amount FROM invoice_lines WHERE invoice_id=$1::bigint ORDER BY id ASC`,
      [id]
    );

    function csvCell(v) {
      const s = (v === null || v === undefined) ? "" : String(v);
      if (/[;"\n\r]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
      return s;
    }

    let csv = "\ufeff" + ["description;quantity;unit;unit_price;amount"].join("\n");
    for (const r of lines.rows) {
      csv += "\n" + [
        csvCell(r.description),
        csvCell(r.quantity),
        csvCell(r.unit),
        csvCell(r.unit_price),
        csvCell(r.amount),
      ].join(";");
    }

    const r0 = inv.rows[0];

    const startIso =
      r0.period_start instanceof Date ? r0.period_start.toISOString().slice(0, 10) : String(r0.period_start).slice(0, 10);
    const endIso =
      r0.period_end instanceof Date ? r0.period_end.toISOString().slice(0, 10) : String(r0.period_end).slice(0, 10);

    const safePo = String(r0.customer_po || "PO").replace(/[^0-9A-Za-z_\-]/g, "_");
    const safeNum = String(r0.invoice_number || ("draft_" + r0.id)).replace(/[^0-9A-Za-z_\-]/g, "_");
    const filename = `invoice_${safeNum}_${safePo}_${startIso}_to_${endIso}.csv`;

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.send(csv);
  } catch (e) {
    console.error("INVOICE CSV ERROR:", e);
    res.status(500).send(e.message || "csv error");
  }
});
// ======================================================
// A8: INVOICES - get invoice + lines
// GET /api/admin/invoices/:id
// ======================================================
app.get("/api/admin/invoices/:id", async (req, res) => {
  try {
    const id = String(req.params.id || "").trim();
    if (!/^\d+$/.test(id)) return res.status(400).json({ ok: false, error: "id ungültig" });

    const inv = await pool.query(`SELECT * FROM invoices WHERE id=$1::bigint`, [id]);
    if (!inv.rowCount) return res.status(404).json({ ok: false, error: "Invoice nicht gefunden" });

    const lines = await pool.query(
      `SELECT * FROM invoice_lines WHERE invoice_id=$1::bigint ORDER BY id ASC`,
      [id]
    );

    res.json({ ok: true, invoice: inv.rows[0], lines: lines.rows });
  } catch (e) {
    console.error("GET INVOICE ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// A8: INVOICES - finalize (assign invoice_number + lock)
// POST /api/admin/invoices/:id/finalize
// Body: { }
// ======================================================
app.post("/api/admin/invoices/:id/finalize", async (req, res) => {
  const client = await pool.connect();
  try {
    // admin guard already applies for /api/admin/*, but keep consistent:
    // (optional) requireCode2012(req);

    const id = String(req.params.id || "").trim();
    if (!/^\d+$/.test(id)) return res.status(400).json({ ok: false, error: "id ungültig" });

    await client.query("BEGIN");

    const inv = await client.query(`SELECT * FROM invoices WHERE id=$1::bigint FOR UPDATE`, [id]);
    if (!inv.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ ok: false, error: "Invoice nicht gefunden" });
    }

    const row = inv.rows[0];
    if (row.status !== "draft") {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: `Finalize nur möglich bei status=draft (aktuell: ${row.status})` });
    }

    // Must have at least one line
    const lines = await client.query(`SELECT COUNT(*)::int AS cnt FROM invoice_lines WHERE invoice_id=$1::bigint`, [id]);
    if ((lines.rows?.[0]?.cnt || 0) <= 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ ok: false, error: "Invoice hat keine Positionen (invoice_lines leer)" });
    }

    const year = new Date().getUTCFullYear();

    // bump counter safely
    const c = await client.query(
      `
      INSERT INTO invoice_counters (year, last_number)
      VALUES ($1, 1)
      ON CONFLICT (year)
      DO UPDATE SET last_number = invoice_counters.last_number + 1
      RETURNING last_number
      `,
      [year]
    );

    const last_number = Number(c.rows[0].last_number);
    const invoice_number = `INV-${year}-${String(last_number).padStart(6, "0")}`;

    await client.query(
      `
      UPDATE invoices
      SET invoice_number=$2,
          status='final',
          finalized_at=NOW()
      WHERE id=$1::bigint
      `,
      [id, invoice_number]
    );

    await client.query("COMMIT");

    return res.json({ ok: true, invoice_id: id, status: "final", invoice_number });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("FINALIZE INVOICE ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// ======================================================
// A9: AUTOMATION - run billing draft creation from REAL TIMES (v_time_entries_clamped)
// POST /api/admin/automation/run
// Body:
// {
//   "mode": "monthly" | "weekly" | "project",
//   "date": "YYYY-MM-DD",              // required for monthly/weekly
//   "from": "YYYY-MM-DD",              // required for project
//   "to": "YYYY-MM-DD",                // required for project
//   "customer_pos": ["..."]            // optional (else: auto-detect POs with hours)
// }
// Rules: if no hours for a PO in range => skip (Option A)
// ======================================================
app.post("/api/admin/automation/run", async (req, res) => {
  const client = await pool.connect();
  try {
    const mode = String(req.body?.mode || "").trim(); // monthly|weekly|project
    const date = req.body?.date != null ? String(req.body.date).trim() : null;
    const fromIn = req.body?.from != null ? String(req.body.from).trim() : null;
    const toIn = req.body?.to != null ? String(req.body.to).trim() : null;

    if (!["monthly", "weekly", "project"].includes(mode)) {
      return res.status(400).json({ ok: false, error: "mode muss monthly|weekly|project sein" });
    }

    function isIso(d) { return /^\d{4}-\d{2}-\d{2}$/.test(String(d || "")); }

    // compute range
    let from, to;
    if (mode === "project") {
      if (!isIso(fromIn) || !isIso(toIn) || toIn < fromIn) {
        return res.status(400).json({ ok: false, error: "project braucht from/to (YYYY-MM-DD), to>=from" });
      }
      from = fromIn;
      to = toIn;
    } else {
      if (!isIso(date)) return res.status(400).json({ ok: false, error: "monthly/weekly braucht date (YYYY-MM-DD)" });

      const d = new Date(date + "T00:00:00.000Z");

      if (mode === "monthly") {
        const y = d.getUTCFullYear();
        const m = d.getUTCMonth();
        const first = new Date(Date.UTC(y, m, 1));
        const last = new Date(Date.UTC(y, m + 1, 0));
        from = first.toISOString().slice(0, 10);
        to = last.toISOString().slice(0, 10);
      }

      if (mode === "weekly") {
        // ISO week: Monday..Sunday
        const day = d.getUTCDay() || 7; // 1..7
        const monday = new Date(d);
        monday.setUTCDate(d.getUTCDate() - (day - 1));
        const sunday = new Date(monday);
        sunday.setUTCDate(monday.getUTCDate() + 6);
        from = monday.toISOString().slice(0, 10);
        to = sunday.toISOString().slice(0, 10);
      }
    }

    // customer_pos: optional
    const bodyPos = Array.isArray(req.body?.customer_pos)
      ? req.body.customer_pos.map(x => String(x).trim()).filter(Boolean)
      : null;

    await client.query("BEGIN");

    // Determine which POs have real hours in range (Option A: skip zero)
    // Use v_time_entries_clamped as single source of truth
    let pos = bodyPos;

    if (!pos || pos.length === 0) {
      const qPos = await client.query(
        `
        SELECT mapped_customer_po AS customer_po
        FROM v_time_entries_clamped
        WHERE work_date BETWEEN $1::date AND $2::date
          AND clamped_hours IS NOT NULL
          AND COALESCE(mapped_customer_po,'') <> ''
        GROUP BY mapped_customer_po
        ORDER BY mapped_customer_po ASC
        `,
        [from, to]
      );
      pos = qPos.rows.map(r => r.customer_po).filter(Boolean);
    }

    const created = [];
    const skipped = [];

    for (const customer_po of pos) {
      // bill_travel for this PO
      const bt = await client.query(
        `SELECT bool_or(bill_travel) AS bill_travel FROM po_work_rules WHERE customer_po = $1`,
        [customer_po]
      );
      const bill_travel = !!bt.rows?.[0]?.bill_travel;

      // Aggregate per employee + internal_po (like your existing invoice CSV logic)
      const daily = await client.query(
        `
        SELECT
          te.employee_id,
          COALESCE(e.name, te.employee_id) AS employee_name,
          COALESCE(te.mapped_internal_po,'') AS internal_po,
          COUNT(DISTINCT te.work_date)::int AS days,
          SUM(te.clamped_hours)::numeric AS hours_raw,
          SUM(COALESCE(te.travel_hours,0))::numeric AS travel_raw
        FROM v_time_entries_clamped te
        LEFT JOIN employees e ON e.employee_id = te.employee_id
        WHERE te.work_date BETWEEN $1::date AND $2::date
          AND te.clamped_hours IS NOT NULL
          AND te.mapped_customer_po = $3
        GROUP BY te.employee_id, employee_name, COALESCE(te.mapped_internal_po,'')
        ORDER BY te.employee_id ASC, internal_po ASC
        `,
        [from, to, customer_po]
      );

      // Option A: if no rows => skip
      if (!daily.rowCount) {
        skipped.push({ customer_po, reason: "no_hours_in_range" });
        continue;
      }

      // total billed hours = clamped_hours + (optional) travel_hours
      let total = 0;
      const lines = [];

      // best-effort customer label
      const cust = await client.query(
        `
        SELECT MAX(customer) AS customer
        FROM staffplan
        WHERE customer_po = $1 AND customer IS NOT NULL AND customer <> ''
        `,
        [customer_po]
      );
      const customer = cust.rows?.[0]?.customer || null;

      for (const r of daily.rows) {
        const hours = Math.round((Number(r.hours_raw) || 0) * 100) / 100;
        const travel = Math.round((Number(r.travel_raw) || 0) * 100) / 100;
        const travel_billed = bill_travel ? travel : 0;

        if (hours > 0) {
          lines.push({
            description: `Arbeitszeit – ${r.employee_name} (${r.employee_id})${r.internal_po ? ` / Internal PO: ${r.internal_po}` : ""} / Tage: ${r.days}`,
            quantity: hours,
            unit: "h",
            unit_price: null,
            amount: hours,
          });
          total += hours;
        }

        if (travel_billed > 0) {
          lines.push({
            description: `Reisezeit – ${r.employee_name} (${r.employee_id})${r.internal_po ? ` / Internal PO: ${r.internal_po}` : ""} / Tage: ${r.days}`,
            quantity: travel_billed,
            unit: "h",
            unit_price: null,
            amount: travel_billed,
          });
          total += travel_billed;
        }
      }

      // If total is 0 after travel rule => skip (still Option A)
      total = Math.round(total * 100) / 100;
      if (total <= 0 || lines.length === 0) {
        skipped.push({ customer_po, reason: "total_zero_after_rules" });
        continue;
      }

      // create invoice draft (duplicate-safe)
      let invoice_id = null;
      try {
        const inv = await client.query(
          `
          INSERT INTO invoices (customer_po, customer, period_start, period_end, status, currency, total_amount, source)
          VALUES ($1, $2, $3::date, $4::date, 'draft', 'EUR', $5, $6)
          RETURNING id
          `,
          [customer_po, customer, from, to, total, "clamped"]
        );
        invoice_id = inv.rows[0].id;
      } catch (e) {
        if (String(e.code) === "23505") {
          skipped.push({ customer_po, reason: "duplicate_po_period_source" });
          continue; // next PO
        }
        throw e;
      }

      for (const li of lines) {
        await client.query(
          `
          INSERT INTO invoice_lines (invoice_id, description, quantity, unit, unit_price, amount)
          VALUES ($1,$2,$3,$4,$5,$6)
          `,
          [invoice_id, li.description, li.quantity, li.unit, li.unit_price, li.amount]
        );
      }

      created.push({ invoice_id: String(invoice_id), customer_po, total_amount: total, lines: lines.length });
    }

    // save run log (for notifications) - keep inside same transaction
    try {
      await client.query(
        `
        INSERT INTO automation_runs
          (mode, period_start, period_end, created_count, skipped_count, created_json, skipped_json, note)
        VALUES
          ($1, $2::date, $3::date, $4, $5, $6::jsonb, $7::jsonb, $8)
        `,
        [
          mode,
          from,
          to,
          created.length,
          skipped.length,
          JSON.stringify(created || []),
          JSON.stringify(skipped || []),
          "automation/run logged",
        ]
      );
    } catch (e) {
      console.warn("automation_runs insert failed:", e.message);
    }

    await client.query("COMMIT");

    return res.json({
      ok: true,
      mode,
      period: { from, to },
      created_count: created.length,
      skipped_count: skipped.length,
      created,
      skipped,
      note: "Draft-only. Quelle: v_time_entries_clamped. Wenn keine Stunden -> keine Rechnung (Option A).",
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error("AUTOMATION RUN ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// ======================================================
// A9.9: CRON endpoints (weekly + monthly)
// ======================================================
app.get("/api/admin/cron/weekly", async (req, res) => {
  try {
    const today = todayIsoBerlin();
    req.body = { mode: "weekly", date: today };
    return app._router.handle(req, res, () => {});
  } catch (e) {
    console.error("CRON WEEKLY ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/admin/cron/monthly", async (req, res) => {
  try {
    const today = todayIsoBerlin();
    req.body = { mode: "monthly", date: today };
    return app._router.handle(req, res, () => {});
  } catch (e) {
    console.error("CRON MONTHLY ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// A9.12: Get last automation run
// ======================================================
app.get("/api/admin/automation/last-run", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT *
      FROM automation_runs
      ORDER BY created_at DESC
      LIMIT 1
    `);

    if (!r.rowCount) return res.json({ ok: true, run: null });

    res.json({ ok: true, run: r.rows[0] });
  } catch (e) {
    console.error("LAST RUN ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});
// ======================================================
// ADMIN DEBUG: A10 PO Coverage
// ======================================================
app.get("/api/admin/debug/a10-po-coverage", async (req, res) => {
  try {
    const from = String(req.query.from || "").trim();
    const to = String(req.query.to || "").trim();
    const customer_po = String(req.query.customer_po || "").trim();

    if (!/^\d{4}-\d{2}-\d{2}$/.test(from) || !/^\d{4}-\d{2}-\d{2}$/.test(to)) {
      return res.status(400).json({ ok: false, error: "from/to YYYY-MM-DD required" });
    }
    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po required" });

    const q = await pool.query(
      `
      WITH te_base AS (
        SELECT te.employee_id, te.work_date::date AS work_date
        FROM time_entries te
        WHERE te.work_date BETWEEN $1::date AND $2::date
          AND te.start_ts IS NOT NULL AND te.end_ts IS NOT NULL
      ),
      te_proj AS (
        SELECT
          b.employee_id,
          b.work_date,
          (
            SELECT NULLIF(TRIM(e.project_id), '')
            FROM time_events e
            WHERE e.employee_id = b.employee_id
              AND (e.event_time AT TIME ZONE 'Europe/Berlin')::date = b.work_date
              AND e.event_type='clock_in'
              AND e.project_id IS NOT NULL
            ORDER BY e.event_time DESC
            LIMIT 1
          ) AS project_id
        FROM te_base b
      ),
      joined AS (
        SELECT
          b.employee_id,
          b.work_date,
          sp.customer_po AS sp_po,
          tp.project_id,
          p.customer_po AS p_po
        FROM te_base b
        LEFT JOIN staffplan sp
          ON sp.employee_id=b.employee_id AND sp.work_date=b.work_date
        LEFT JOIN te_proj tp
          ON tp.employee_id=b.employee_id AND tp.work_date=b.work_date
        LEFT JOIN projects p
          ON TRIM(p.project_id)=TRIM(tp.project_id)
      )
      SELECT
        (SELECT COUNT(*)::int FROM te_base) AS time_entries_days,
        (SELECT COUNT(*)::int FROM joined WHERE NULLIF(TRIM(project_id),'') IS NOT NULL) AS with_project_id,
        (SELECT COUNT(*)::int FROM joined WHERE NULLIF(TRIM(sp_po),'') IS NOT NULL) AS with_staffplan_po,
        (SELECT COUNT(*)::int FROM joined WHERE NULLIF(TRIM(p_po),'') IS NOT NULL) AS with_projects_po,
        (SELECT COUNT(*)::int FROM joined
          WHERE regexp_replace(COALESCE(sp_po, p_po, ''), '\\s', '', 'g') = regexp_replace($3, '\\s', '', 'g')
        ) AS match_po_count
      `,
      [from, to, customer_po]
    );

    return res.json({ ok: true, ...q.rows[0] });
  } catch (e) {
    console.error("A10 PO COVERAGE ERROR:", e);
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
