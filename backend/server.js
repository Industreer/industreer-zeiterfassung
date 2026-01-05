console.log("🔥🔥🔥 SERVER.JS FULL FINAL + SAFE IMPORT + SCAN-DATES + SCAN-VALUES + UPSERT + TARGET END COL FIX + OPTIMIZED IMPORT LOOP + INDEXES + STATS + DRY-RUN 🔥🔥🔥");

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
  return d.toISOString().slice(0, 10);
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

  // Datum irgendwo im Text
  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // DD.MM. (ohne Jahr)
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

  // "Sa 27.12."
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

async function ensureColumn(table, column, typeSql) {
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='${table}' AND column_name='${column}'
      ) THEN
        ALTER TABLE ${table} ADD COLUMN ${column} ${typeSql};
      END IF;
    END $$;
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

  // Unique für UPSERT
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS staffplan_uniq
    ON staffplan (employee_id, work_date, customer_po, internal_po, project_short);
  `);

  // Performance-Indizes
  await pool.query(`
    CREATE INDEX IF NOT EXISTS staffplan_by_date
    ON staffplan (work_date);

    CREATE INDEX IF NOT EXISTS staffplan_by_date_emp
    ON staffplan (work_date, employee_id);
  `);

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
      activity TEXT,
      break_minutes INT DEFAULT 0
    );
  `);

  await ensureColumn("time_entries", "start_ts", "TIMESTAMPTZ");
  await ensureColumn("time_entries", "end_ts", "TIMESTAMPTZ");
  await ensureColumn("time_entries", "start_time", "TIMESTAMPTZ");
  await ensureColumn("time_entries", "end_time", "TIMESTAMPTZ");
  await ensureColumn("time_entries", "break_minutes", "INT DEFAULT 0");
  await ensureColumn("time_entries", "customer_po", "TEXT");
  await ensureColumn("time_entries", "internal_po", "TEXT");
  await ensureColumn("time_entries", "project_short", "TEXT");
  await ensureColumn("time_entries", "requester_name", "TEXT");
  await ensureColumn("time_entries", "activity", "TEXT");

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

async function getEmployeeNameById(employeeId) {
  const r = await pool.query(`SELECT name FROM employees WHERE employee_id=$1`, [employeeId]);
  return r.rowCount ? r.rows[0].name : null;
}

async function getTodayProjectsSmart(employeeId, dateISO) {
  const r1 = await pool.query(
    `
    SELECT work_date, calendar_week, customer, requester_name, internal_po, customer_po, project_short, planned_hours
    FROM staffplan
    WHERE employee_id=$1 AND work_date=$2::date
    ORDER BY customer_po, internal_po
    `,
    [employeeId, dateISO]
  );
  if (r1.rows.length) return { rows: r1.rows, matched: "employee_id" };

  const empName = await getEmployeeNameById(employeeId);
  if (!empName) return { rows: [], matched: "none" };

  const r2 = await pool.query(
    `
    SELECT work_date, calendar_week, customer, requester_name, internal_po, customer_po, project_short, planned_hours
    FROM staffplan
    WHERE work_date=$1::date
      AND lower(regexp_replace(trim(
          CASE
            WHEN position(',' in employee_name) > 0
              THEN trim(split_part(employee_name, ',', 2)) || ' ' || trim(split_part(employee_name, ',', 1))
            ELSE employee_name
          END
        ), '\\s+', ' ', 'g'))
        = lower(regexp_replace(trim($2), '\\s+', ' ', 'g'))
    ORDER BY customer_po, internal_po
    `,
    [dateISO, empName]
  );

  return { rows: r2.rows, matched: r2.rows.length ? "name_fallback" : "none" };
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
    build:
      "server.js FULL FINAL + SAFE IMPORT + SCAN-DATES + SCAN-VALUES + UPSERT + TARGET END COL FIX + OPTIMIZED IMPORT LOOP + INDEXES + STATS + DRY-RUN",
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

app.get("/api/employee/:id", async (req, res) => {
  const r = await pool.query(
    `SELECT employee_id,name,email,language FROM employees WHERE employee_id=$1`,
    [req.params.id]
  );
  if (!r.rowCount) return res.status(404).json({ ok: false });
  res.json({ ok: true, employee: r.rows[0] });
});

// ======================================================
// EMPLOYEE TODAY
// ======================================================
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const dateOverride = String(req.query.date || "").trim();
    const dateISO = dateOverride || new Date().toISOString().slice(0, 10);

    const r = await getTodayProjectsSmart(employeeId, dateISO);

    return res.json({
      ok: true,
      date: dateISO,
      matched: r.matched,
      projects: r.rows,
    });
  } catch (e) {
    console.error("EMPLOYEE TODAY ERROR:", e);
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
      const d = parsed ? parsed : new Date(currentBaseDate.getTime() + (c - currentBaseCol) * 86400000);
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
// STAFFPLAN IMPORT (STATS + DRY-RUN)
// POST /api/import/staffplan?reset=1
// POST /api/import/staffplan?dry_run=1
// Optional: &target_end=YYYY-MM-DD
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const isDryRun = String(req.query.dry_run || "") === "1";

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
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
      return res.json({ ok: false, error: "Keine Datums-Kopfzeile gefunden (Scan Zeilen 1..26)" });
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
    if (!baseDate || firstDateCol === null) {
      return res.json({ ok: false, error: "Header gefunden, aber kein erstes Datum parsebar" });
    }

    // ensure endCol covers target_end column
    const targetEndIso = String(req.query.target_end || "2025-12-27").trim();
    const baseIso = toIsoDate(baseDate);
    const base = new Date(baseIso + "T00:00:00.000Z");
    const targetEnd = new Date(targetEndIso + "T00:00:00.000Z");

    if (isFinite(targetEnd.getTime())) {
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

    console.log(
      "📅 Import headerRow:", headerRow + 1,
      "Dates:", dates[0]?.iso, "…", dates[dates.length - 1]?.iso,
      "cols:", dates.length,
      "endCol:", endCol,
      "target_end:", targetEndIso,
      "dry_run:", isDryRun
    );

    // Reset nur wenn NICHT dry-run
    if (!isDryRun && String(req.query.reset) === "1") {
      console.log("🧹 Reset=1 -> TRUNCATE staffplan");
      await pool.query("TRUNCATE staffplan");
    }

    // DRY-RUN: existing keys für Date-Range laden, damit wir would_insert vs would_update zählen können
    // (wenn reset=1 im echten Import, dann wäre alles insert — aber im Dry-Run respektieren wir reset nicht, weil wir nicht schreiben)
    let existingKeySet = null;
    if (isDryRun) {
      existingKeySet = new Set();
      const rExist = await pool.query(
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
    let imported = 0;               // total rows written (real run)
    let inserted_rows = 0;          // real run
    let updated_rows = 0;           // real run

    let would_write_rows = 0;       // dry-run
    let would_insert_rows = 0;      // dry-run
    let would_update_rows = 0;      // dry-run

    let skippedNoEmployee = 0;

    // OPTIMIZATION: Break when long empty streak (file end reached)
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
        if (emptyEmployeeStreak >= EMPTY_STREAK_BREAK) {
          console.log(`🧠 Import stop: ${EMPTY_STREAK_BREAK} leere Mitarbeiterzeilen am Stück (ab Zeile ${r + 1})`);
          break;
        }
        continue;
      }
      emptyEmployeeStreak = 0;

      const employeeNameCanonical = commaSwapName(employeeNameRaw);

      // employee_id (für dry-run dürfen wir employees nicht anlegen → wir simulieren AUTO-ID wenn nicht vorhanden)
      const n1 = normalizeName(employeeNameRaw);
      const n2 = normalizeName(employeeNameCanonical);

      const emp = await pool.query(
        `
        SELECT employee_id, name
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

        if (!isDryRun) {
          await pool.query(
            `
            INSERT INTO employees (employee_id, name)
            VALUES ($1, $2)
            ON CONFLICT (employee_id) DO UPDATE SET name = EXCLUDED.name
            `,
            [employeeId, employeeNameCanonical]
          );
        }
      }

      // Customer / POs
      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v || null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v || null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v || null;

      for (const d of dates) {
        const proj = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v || null;

        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;
        const plan = (typeof planRaw === "number" && isFinite(planRaw)) ? planRaw : null;

        if (!proj && plan === null) continue;

        const key = `${employeeId}#${d.iso}#${customerPo || ""}#${internalPo || ""}#${proj || ""}`;

        if (isDryRun) {
          would_write_rows++;
          if (existingKeySet && existingKeySet.has(key)) would_update_rows++;
          else would_insert_rows++;
          continue;
        }

        // Real run: UPSERT + RETURNING inserted flag via xmax
        const q = await pool.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, requester_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
          ON CONFLICT (employee_id, work_date, customer_po, internal_po, project_short)
          DO UPDATE SET
            employee_name  = EXCLUDED.employee_name,
            requester_name = EXCLUDED.requester_name,
            calendar_week  = EXCLUDED.calendar_week,
            customer       = EXCLUDED.customer,
            planned_hours  = EXCLUDED.planned_hours
          RETURNING (xmax = 0) AS inserted
          `,
          [employeeId, employeeNameCanonical, requesterName, d.iso, d.cw, customer, internalPo, customerPo, proj, plan]
        );

        imported++;
        if (q.rowCount && q.rows[0]?.inserted) inserted_rows++;
        else updated_rows++;
      }
    }

    return res.json({
      ok: true,
      mode: isDryRun ? "dry_run" : "write",
      imported: isDryRun ? 0 : imported,
      inserted_rows: isDryRun ? 0 : inserted_rows,
      updated_rows: isDryRun ? 0 : updated_rows,

      would_write_rows: isDryRun ? would_write_rows : 0,
      would_insert_rows: isDryRun ? would_insert_rows : 0,
      would_update_rows: isDryRun ? would_update_rows : 0,

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
      note: isDryRun
        ? "Dry-run: keine DB-Änderung. would_* basiert auf bestehenden Keys in staffplan innerhalb des Date-Ranges."
        : "Write: inserted_rows/updated_rows kommen aus RETURNING (xmax=0) bei UPSERT.",
    });
  } catch (e) {
    console.error("STAFFPLAN IMPORT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: staffplan
// ======================================================
app.get("/api/debug/staffplan-minmax", async (req, res) => {
  const r = await pool.query(`
    SELECT MIN(work_date) AS min_date,
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
          AND lower(regexp_replace(trim(
              CASE
                WHEN position(',' in employee_name) > 0
                  THEN trim(split_part(employee_name, ',', 2)) || ' ' || trim(split_part(employee_name, ',', 1))
                ELSE employee_name
              END
            ), '\\s+', ' ', 'g'))
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
