const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// -------------------- middleware --------------------
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// -------------------- pages --------------------
app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

// -------------------- database --------------------
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false },
});

// -------------------- upload --------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 },
});

// -------------------- logo store --------------------
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

// ======================================================================
// Helpers
// ======================================================================
function pad2(n) {
  return String(n).padStart(2, "0");
}

// Parse dates from Excel cells (Date object / serial / "dd.mm.yyyy")
function parseDateAny(v) {
  if (!v) return null;

  if (v instanceof Date && !isNaN(v)) return v.toISOString().slice(0, 10);

  if (typeof v === "number") {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  if (typeof v === "string") {
    const m = v.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
    if (m) return `${m[3]}-${pad2(m[2])}-${pad2(m[1])}`;
  }

  return null;
}

// Parse hours from Excel cell (number or "7,5" etc.)
function parseHoursAny(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && isFinite(v)) return v;
  if (typeof v === "string") {
    const m = v.replace(",", ".").match(/(\d+(\.\d+)?)/);
    if (m) return Number(m[1]);
  }
  return null;
}

// ISO week (Mon-Sun), formatted CWxx
function isoWeekNumber(dateObj) {
  const d = new Date(Date.UTC(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return weekNo;
}
function cwFromISODate(isoDateStr) {
  const [y, m, d] = isoDateStr.split("-").map(Number);
  const dt = new Date(y, m - 1, d);
  return `CW${isoWeekNumber(dt)}`;
}

// Safe string
function s(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

// ======================================================================
// Migrations
// ======================================================================
async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id SERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      start_time TIMESTAMP NOT NULL,
      end_time TIMESTAMP,
      total_hours NUMERIC(10,2),
      activity TEXT DEFAULT 'Arbeitszeit'
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS break_entries (
      id SERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      time_entry_id INTEGER,
      start_time TIMESTAMP NOT NULL,
      end_time TIMESTAMP
    );
  `);
  await pool.query(`ALTER TABLE break_entries ADD COLUMN IF NOT EXISTS time_entry_id INTEGER;`);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT,
      employee_name TEXT,
      work_date DATE,
      customer_name TEXT,
      customer_po TEXT,
      internal_po TEXT,
      project_code TEXT,
      planned_hours NUMERIC(10,2)
    );
  `);

  // Ensure columns exist for older DBs
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS calendar_week TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS employee_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS work_date DATE;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS project_code TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS planned_hours NUMERIC(10,2);`);
}

// ======================================================================
// Basic endpoints
// ======================================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// -------------------- logo --------------------
app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });
  if (!["image/png", "image/jpeg"].includes(req.file.mimetype)) {
    return res.status(400).json({ ok: false, error: "Nur PNG oder JPG erlaubt" });
  }
  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(LOGO_META, JSON.stringify({ mimetype: req.file.mimetype }));
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE) || !fs.existsSync(LOGO_META)) return res.sendStatus(404);
  const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ======================================================================
// Employees API (Admin view + update) + Lookup
// ======================================================================
app.get("/api/employees", async (_req, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees ORDER BY name"
  );
  res.json(r.rows);
});

app.get("/api/employee/:id", async (req, res) => {
  const id = s(req.params.id);
  if (!id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1",
    [id]
  );
  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

  res.json({ ok: true, employee: r.rows[0] });
});

// Update employee fields; supports later ID change via new_employee_id
app.post("/api/employees/update", async (req, res) => {
  const employee_id = s(req.body.employee_id);
  const new_employee_id = s(req.body.new_employee_id) || null;
  const email = (req.body.email === undefined) ? undefined : (s(req.body.email) || null);
  const language = (req.body.language === undefined) ? undefined : (s(req.body.language) || null);

  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    let effectiveId = employee_id;

    if (new_employee_id && new_employee_id !== employee_id) {
      const exists = await client.query("SELECT 1 FROM employees WHERE employee_id=$1", [new_employee_id]);
      if (exists.rows.length) {
        await client.query("ROLLBACK");
        return res.status(400).json({ ok: false, error: "Neue ID existiert bereits" });
      }
      await client.query("UPDATE employees SET employee_id=$1 WHERE employee_id=$2", [new_employee_id, employee_id]);
      await client.query("UPDATE time_entries SET employee_id=$1 WHERE employee_id=$2", [new_employee_id, employee_id]);
      await client.query("UPDATE break_entries SET employee_id=$1 WHERE employee_id=$2", [new_employee_id, employee_id]);
      effectiveId = new_employee_id;
    }

    if (email !== undefined) {
      await client.query("UPDATE employees SET email=$1 WHERE employee_id=$2", [email, effectiveId]);
    }
    if (language !== undefined) {
      await client.query("UPDATE employees SET language=$1 WHERE employee_id=$2", [language, effectiveId]);
    }

    await client.query("COMMIT");
    res.json({ ok: true });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// ======================================================================
// Auto-create employees from staffplan names (keeps your workflow working)
// ======================================================================
async function getNextNumericEmployeeId(client) {
  const r = await client.query(
    `SELECT MAX(employee_id::int) AS max_id
     FROM employees
     WHERE employee_id ~ '^[0-9]+$'`
  );
  const maxId = r.rows[0]?.max_id;
  return (maxId ? Number(maxId) + 1 : 1000);
}

async function ensureEmployeeByName(client, name, idCache) {
  const n = s(name);
  if (!n) return null;
  if (idCache.has(n)) return idCache.get(n);

  // find by exact name
  const found = await client.query(
    "SELECT employee_id FROM employees WHERE name=$1 LIMIT 1",
    [n]
  );
  if (found.rows.length) {
    idCache.set(n, found.rows[0].employee_id);
    return found.rows[0].employee_id;
  }

  // create new numeric ID
  const nextId = await getNextNumericEmployeeId(client);
  await client.query(
    "INSERT INTO employees (employee_id, name, email, language) VALUES ($1,$2,NULL,'de')",
    [String(nextId), n]
  );
  idCache.set(n, String(nextId));
  return String(nextId);
}

// ======================================================================
// Timer support: running work block
// ======================================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  const employee_id = s(req.params.employee_id);
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT id, start_time
     FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC
     LIMIT 1`,
    [employee_id, today]
  );

  if (!r.rows.length) return res.json({ ok: false });
  res.json({ ok: true, time_entry_id: r.rows[0].id, start_time: r.rows[0].start_time });
});

// ======================================================================
// STAFFPLAN IMPORT (ROBUST, OPTION A) + 2-row logic:
// - employee_name in K or I
// - per date column:
//    row: project_code (text)  (O8 style)
//    row+1: planned_hours (number) (O9 style)
// - import row/day only if BOTH present: project_code != "" AND planned_hours > 0
// - calendar_week ALWAYS from date (ISO)
// ======================================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  const client = await pool.connect();
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    // 1) Detect date header row by scanning first ~40 rows and many columns to the right
    //    Start at column L (index 11), go far right (e.g. 260 columns)
    let dateHeaderRow = null;
    let dates = [];

    const maxRightCols = 260; // very wide sheets (supports years)
    const startCol = 11; // L

    for (let r = 1; r <= 40; r++) {
      const found = [];
      for (let c = startCol; c < startCol + maxRightCols; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: r - 1, c })];
        const iso = cell ? parseDateAny(cell.v) : null;
        if (iso) found.push({ c, iso });
      }
      // Heuristic: a date header row usually contains many dates; accept if >= 5
      if (found.length >= 5) {
        dateHeaderRow = r;
        dates = found;
        break;
      }
    }

    if (!dates.length) {
      return res.json({
        ok: true,
        imported: 0,
        note: "Keine Datumszeile gefunden (erwartet Datumswerte ab Spalte L).",
      });
    }

    // 2) Determine min/max date and delete staffplan rows in that range for clean reimport
    const isoList = dates.map(d => d.iso).sort();
    const minDate = isoList[0];
    const maxDate = isoList[isoList.length - 1];

    await client.query("BEGIN");
    await client.query(`DELETE FROM staffplan WHERE work_date BETWEEN $1::date AND $2::date`, [minDate, maxDate]);

    // 3) Import all staff rows
    let imported = 0;
    const weeksDetected = new Set();
    const employeeIdCache = new Map();

    // We'll scan many rows – your file can be huge. We stop after long empty streak of employees.
    let emptyEmployeeStreak = 0;

    for (let row = 6; row < 12000; row++) {
      // Employee name can be in K or I (robust)
      const employee_name = s(ws[`K${row}`]?.v) || s(ws[`I${row}`]?.v);
      if (!employee_name) {
        emptyEmployeeStreak++;
        if (emptyEmployeeStreak > 400) break; // stop if too many empty rows
        continue;
      }
      emptyEmployeeStreak = 0;

      // Customer / PO fields on the employee row
      const customer_name = s(ws[`A${row}`]?.v);
      const internal_po = s(ws[`B${row}`]?.v);
      const customer_po = s(ws[`E${row}`]?.v);

      // Optional: auto-create employee records so login works without separate import
      await ensureEmployeeByName(client, employee_name, employeeIdCache);

      for (const d of dates) {
        const iso = d.iso;
        const calendar_week = cwFromISODate(iso);
        weeksDetected.add(calendar_week);

        // project_code on row (e.g. O8)
        const codeCell = ws[XLSX.utils.encode_cell({ r: row - 1, c: d.c })];
        const project_code = s(codeCell?.v);

        // planned_hours on next row (e.g. O9)
        const plannedCell = ws[XLSX.utils.encode_cell({ r: row, c: d.c })]; // row+1 in 1-based => r=row (0-based index)
        const planned_hours = parseHoursAny(plannedCell?.v);

        // Only import if BOTH present (your rule: text + number)
        if (!project_code) continue;
        if (!planned_hours || planned_hours <= 0) continue;

        await client.query(
          `INSERT INTO staffplan
           (calendar_week, employee_name, work_date, customer_name, customer_po, internal_po, project_code, planned_hours)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [calendar_week, employee_name, iso, customer_name, customer_po, internal_po, project_code, planned_hours]
        );
        imported++;
      }
    }

    await client.query("COMMIT");

    res.json({
      ok: true,
      imported,
      dateRange: { from: minDate, to: maxDate },
      weeksDetected: Array.from(weeksDetected).sort((a, b) => {
        const na = Number(a.replace("CW", "")) || 0;
        const nb = Number(b.replace("CW", "")) || 0;
        return na - nb;
      }),
    });
  } catch (e) {
    try { await client.query("ROLLBACK"); } catch {}
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  } finally {
    client.release();
  }
});

// Optional: clear staffplan
app.post("/api/admin/staffplan/clear", async (_req, res) => {
  await pool.query("DELETE FROM staffplan");
  res.json({ ok: true });
});

// ======================================================================
// OPTION B: All POs of an employee for a KW (uses employee_name mapped from employees.name)
// ======================================================================
app.get("/api/employee/:employeeId/pos/:kw", async (req, res) => {
  try {
    const employeeId = s(req.params.employeeId);
    const kw = s(req.params.kw);

    const emp = await pool.query("SELECT name FROM employees WHERE employee_id=$1", [employeeId]);
    if (!emp.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

    const employeeName = emp.rows[0].name;

    const r = await pool.query(
      `SELECT DISTINCT customer_po
       FROM staffplan
       WHERE calendar_week=$1
         AND employee_name=$2
         AND customer_po IS NOT NULL
         AND customer_po <> ''
       ORDER BY customer_po`,
      [kw, employeeName]
    );

    res.json({ ok: true, pos: r.rows.map(x => x.customer_po) });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================================
// Time tracking: multi blocks + breaks
// ======================================================================
app.post("/api/time/start", async (req, res) => {
  const employee_id = s(req.body.employee_id);
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  const open = await pool.query(
    `SELECT id FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (open.rows.length) return res.status(400).json({ ok: false, error: "Es läuft bereits ein Arbeitsblock" });

  const ins = await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())
     RETURNING id, start_time`,
    [employee_id, today]
  );

  res.json({ ok: true, time_entry_id: ins.rows[0].id, start_time: ins.rows[0].start_time });
});

app.post("/api/break/start", async (req, res) => {
  const employee_id = s(req.body.employee_id);
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  const open = await pool.query(
    `SELECT id FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (!open.rows.length) return res.status(400).json({ ok: false, error: "Kein laufender Arbeitsblock" });

  const openBreak = await pool.query(
    `SELECT id FROM break_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (openBreak.rows.length) return res.status(400).json({ ok: false, error: "Pause läuft bereits" });

  await pool.query(
    `INSERT INTO break_entries (employee_id, work_date, time_entry_id, start_time)
     VALUES ($1,$2,$3,NOW())`,
    [employee_id, today, open.rows[0].id]
  );

  res.json({ ok: true });
});

app.post("/api/break/end", async (req, res) => {
  const employee_id = s(req.body.employee_id);
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT id, start_time FROM break_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (!r.rows.length) return res.status(400).json({ ok: false, error: "Keine aktive Pause" });

  await pool.query(`UPDATE break_entries SET end_time=NOW() WHERE id=$1`, [r.rows[0].id]);

  const mins = Math.round((new Date() - new Date(r.rows[0].start_time)) / 60000);
  res.json({ ok: true, minutes: mins });
});

app.post("/api/time/end", async (req, res) => {
  const employee_id = s(req.body.employee_id);
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const activity = s(req.body.activity) || "Arbeitszeit";
  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (!r.rows.length) return res.status(400).json({ ok: false, error: "Kein laufender Arbeitsblock" });

  const entry = r.rows[0];
  const start = new Date(entry.start_time);
  const end = new Date();

  const b = await pool.query(
    `SELECT start_time, end_time FROM break_entries
     WHERE time_entry_id=$1 AND end_time IS NOT NULL`,
    [entry.id]
  );

  let breakMs = 0;
  for (const br of b.rows) {
    breakMs += new Date(br.end_time) - new Date(br.start_time);
  }

  const netMs = (end - start) - breakMs;
  const netHours = netMs / 3600000;

  await pool.query(
    `UPDATE time_entries
     SET end_time=NOW(), total_hours=$1, activity=$2
     WHERE id=$3`,
    [netHours, activity, entry.id]
  );

  res.json({
    ok: true,
    time_entry_id: entry.id,
    net_hours: Number(netHours).toFixed(2),
    break_minutes: Math.round(breakMs / 60000),
  });
});

// ======================================================================
// PDF Timesheet
// - Shows ALL staffplan days for KW+PO+employee (even if no IST)
// - project_code from staffplan shown in header over customer PO
// - planned_hours shown per day
// - IST aggregated per day: earliest start, latest end, sum net hours, sum breaks, activities joined
// ======================================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  try {
    const employeeId = s(req.params.employeeId);
    const kw = s(req.params.kw);
    const customerPo = s(req.params.customerPo);

    const emp = await pool.query(
      "SELECT name FROM employees WHERE employee_id=$1",
      [employeeId]
    );
    if (!emp.rows.length) return res.sendStatus(404);
    const employeeName = emp.rows[0].name;

    // Staffplan rows for this KW+PO+employee
    const sp = await pool.query(
      `SELECT work_date, customer_name, internal_po, project_code, planned_hours
       FROM staffplan
       WHERE calendar_week=$1 AND customer_po=$2 AND employee_name=$3
       ORDER BY work_date`,
      [kw, customerPo, employeeName]
    );

    if (!sp.rows.length) {
      return res.status(404).send("Keine Staffplan-Daten für diese KW/PO/Mitarbeiter gefunden.");
    }

    const customerName = sp.rows[0].customer_name || "-";
    const internalPo = sp.rows[0].internal_po || "-";

    // Choose a "best" project_code for the header:
    // If multiple different codes across days, show the first + "..."
    const codes = Array.from(new Set(sp.rows.map(r => s(r.project_code)).filter(Boolean)));
    let headerProjectCode = "-";
    if (codes.length === 1) headerProjectCode = codes[0];
    else if (codes.length > 1) headerProjectCode = `${codes[0]} …`;

    const staffDates = sp.rows.map(r => r.work_date);

    // All completed time_entries on these dates
    const te = await pool.query(
      `SELECT id, work_date, start_time, end_time, total_hours, activity
       FROM time_entries
       WHERE employee_id=$1
         AND work_date = ANY($2::date[])
         AND end_time IS NOT NULL
       ORDER BY work_date, start_time`,
      [employeeId, staffDates]
    );

    const entryIds = te.rows.map(x => x.id);
    const br = await pool.query(
      `SELECT time_entry_id, start_time, end_time
       FROM break_entries
       WHERE employee_id=$1
         AND end_time IS NOT NULL
         AND time_entry_id = ANY($2::int[])`,
      [employeeId, entryIds.length ? entryIds : [0]]
    );

    const breakMinutesByEntry = new Map();
    for (const b of br.rows) {
      const mins = Math.round((new Date(b.end_time) - new Date(b.start_time)) / 60000);
      breakMinutesByEntry.set(b.time_entry_id, (breakMinutesByEntry.get(b.time_entry_id) || 0) + mins);
    }

    // Aggregate IST per day
    const istByDate = new Map();
    for (const r of te.rows) {
      const key = new Date(r.work_date).toISOString().slice(0, 10);
      const h = Number(r.total_hours || 0);
      const bm = breakMinutesByEntry.get(r.id) || 0;

      if (!istByDate.has(key)) {
        istByDate.set(key, {
          earliestStart: new Date(r.start_time),
          latestEnd: new Date(r.end_time),
          hours: 0,
          breakMins: 0,
          activities: new Set(),
        });
      }
      const agg = istByDate.get(key);
      const st = new Date(r.start_time);
      const et = new Date(r.end_time);

      if (st < agg.earliestStart) agg.earliestStart = st;
      if (et > agg.latestEnd) agg.latestEnd = et;
      agg.hours += h;
      agg.breakMins += bm;
      agg.activities.add(s(r.activity) || "Arbeitszeit");
    }

    // PDF
    const doc = new PDFDocument({ size: "A4", margin: 40 });
    res.setHeader("Content-Type", "application/pdf");
    doc.pipe(res);

    // Logo centered
    if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
      const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
      const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
      const w = 240;
      doc.image(fs.readFileSync(LOGO_FILE), (doc.page.width - w) / 2, 25, { width: w, format });
    }

    doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });

    doc.font("Helvetica").fontSize(9);
    doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
    doc.text(`Kunde: ${customerName}`, 40, 155);
    doc.text(`Kalenderwoche: ${kw}`, 40, 170);

    // Project code ABOVE customer PO (as requested)
    doc.text(`Projekt (Kurzzeichen): ${headerProjectCode}`, 300, 140);
    doc.text(`Kunden-PO: ${customerPo}`, 300, 155);
    doc.text(`Interne PO: ${internalPo}`, 300, 170);

    // Table
    let y = 200;
    const rowH = 12;

    doc.font("Helvetica-Bold");
    doc.text("Datum", 40, y);
    doc.text("Tätigkeit", 95, y);
    doc.text("Plan", 260, y, { width: 40, align: "right" });
    doc.text("Start", 310, y);
    doc.text("Ende", 355, y);
    doc.text("Pause", 405, y, { width: 45, align: "right" });
    doc.text("IST", 520, y, { align: "right" });

    y += rowH + 3;
    doc.moveTo(40, y).lineTo(550, y).stroke();
    y += 4;
    doc.font("Helvetica");

    let sumIst = 0;
    let sumPlan = 0;
    let sumBreak = 0;

    for (const spRow of sp.rows) {
      const iso = new Date(spRow.work_date).toISOString().slice(0, 10);
      const dateLabel = new Date(spRow.work_date).toLocaleDateString("de-DE");

      const plan = Number(spRow.planned_hours || 0);
      sumPlan += plan;

      const ist = istByDate.get(iso);
      const istHours = ist ? ist.hours : 0;
      const breakMins = ist ? ist.breakMins : 0;
      const act = ist ? Array.from(ist.activities).join(", ").slice(0, 24) : "";

      sumIst += istHours;
      sumBreak += breakMins;

      const st = ist ? ist.earliestStart.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }) : "";
      const et = ist ? ist.latestEnd.toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" }) : "";

      doc.text(dateLabel, 40, y);
      doc.text(act || "", 95, y);
      doc.text(plan ? plan.toFixed(2) : "", 260, y, { width: 40, align: "right" });
      doc.text(st, 310, y);
      doc.text(et, 355, y);
      doc.text(ist ? `${breakMins}m` : "", 405, y, { width: 45, align: "right" });
      doc.text(ist ? istHours.toFixed(2) : "", 520, y, { align: "right" });

      y += rowH;
      if (y > 760) {
        doc.addPage();
        y = 40;
      }
    }

    y += 10;
    doc.font("Helvetica-Bold");
    doc.text("Summe Plan:", 320, y);
    doc.text(sumPlan.toFixed(2), 420, y, { width: 60, align: "right" });
    doc.text("Summe IST:", 460, y);
    doc.text(sumIst.toFixed(2), 520, y, { align: "right" });

    y += 14;
    doc.font("Helvetica");
    doc.text(`Summe Pausen: ${sumBreak} Minuten`, 40, y);

    doc.end();
  } catch (e) {
    console.error(e);
    res.status(500).send("PDF Fehler: " + e.message);
  }
});

// ======================================================================
// Start
// ======================================================================
migrate()
  .then(() => {
    app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
  })
  .catch((e) => {
    console.error("DB migrate failed:", e);
    process.exit(1);
  });
