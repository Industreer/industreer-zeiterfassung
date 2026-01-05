console.log("🔥🔥🔥 SERVER.JS FULL FINAL + SAFE IMPORT + SCAN-DATES + UPSERT 🔥🔥🔥");
/**
 * backend/server.js
 *
 * Fixes / Änderungen:
 * - staffplan Import löscht NICHT mehr automatisch alles (nur wenn ?reset=1)
 * - staffplan Import ist idempotent via UNIQUE INDEX + UPSERT
 * - /api/debug/scan-dates hinzugefügt (für debug.html)
 * - /api/import/staffplan Response enthält date_from/date_to/date_cols/header_row
 *
 * Bestehende Endpoints bleiben erhalten:
 * - /api/employee/today (smart match)
 * - staffplan debug endpoints
 * - time/break endpoints
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
  // "Irrgang, Jens" -> "Jens Irrgang"
  const t = String(s || "").trim();
  if (!t.includes(",")) return t;
  const last = t.split(",")[0].trim();
  const first = t.split(",").slice(1).join(",").trim();
  if (!first || !last) return t;
  return `${first} ${last}`.replace(/\s+/g, " ").trim();
}

function makeAutoIdFromName(name) {
  // stabiler AUTO-Key (kein Konflikt bei Re-Import)
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

  // Datum irgendwo im Text: "Sa 27.12.2025"
  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // DD.MM. (ohne Jahr) -> heuristisch Jahr bestimmen
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

  // "Sa 27.12." -> DD.MM. irgendwo im Text
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

  // Unique Index für UPSERT (idempotent reimport)
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS staffplan_uniq
    ON staffplan (employee_id, work_date, customer_po, internal_po, project_short);
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

  // Backward compatibility
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
  // 1) by employee_id
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

  // 2) name fallback
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
    build: "server.js FULL FINAL + SAFE IMPORT + SCAN-DATES + UPSERT",
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
// EMPLOYEE – HEUTIGE PROJEKTE (SMART MATCH)
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
// DEBUG: Scan Dates (für debug.html)
// ======================================================
app.post("/api/debug/scan-dates", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    let headerRow = null;
    let bestCnt = 0;
    let bestStartCol = null;
    let bestEndCol = null;

    for (let rr = 0; rr <= 25; rr++) {
      let cnt = 0;
      let first = null;
      let last = null;
      for (let c = 0; c <= 260; c++) {
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
      return res.json({
        ok: false,
        error: "Keine Datums-Kopfzeile gefunden",
        bestCnt,
        headerRow,
        bestStartCol,
        bestEndCol,
      });
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
// STAFFPLAN IMPORT (SAFE + UPSERT)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    // --- HeaderRow finden (Zeilen 0..25)
    let headerRow = null;
    let bestCnt = 0;
    let bestStartCol = null;
    let bestEndCol = null;

    for (let rr = 0; rr <= 25; rr++) {
      let cnt = 0;
      let first = null;
      let last = null;
      for (let c = 0; c <= 260; c++) {
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
    const endCol = bestEndCol;

    // --- Dates pro Spalte bauen ---
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

      dates.push({ col: c, iso: toIsoDate(d), cw: "CW" + getISOWeek(d) });
    }

    console.log(
      "📅 Import headerRow:", headerRow + 1,
      "Dates:", dates[0]?.iso, "…", dates[dates.length - 1]?.iso,
      "cols:", dates.length
    );

    // Optionaler Reset
    if (String(req.query.reset) === "1") {
      console.log("🧹 Reset=1 -> TRUNCATE staffplan");
      await pool.query("TRUNCATE staffplan");
    }

    let imported = 0;
    let skippedNoEmployee = 0;

    // Zeilen: Mitarbeiterblock alle 2 Zeilen
    for (let r = 5; r < 20000; r += 2) {
      // Requester I (c=8) – nur wenn wir K als Employee nutzen; sonst kann es konfliktieren
      const requesterCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      const requesterName = requesterCell?.v ? String(requesterCell.v).trim() : null;

      // Mitarbeiter: primär K (10), fallback I (8) falls K leer
      const empCellK = ws[XLSX.utils.encode_cell({ r, c: 10 })];
      const empCellI = ws[XLSX.utils.encode_cell({ r, c: 8 })];

      const employeeNameRaw =
        (empCellK?.v ? String(empCellK.v).trim() : "") ||
        (empCellI?.v ? String(empCellI.v).trim() : "");

      if (!employeeNameRaw) { skippedNoEmployee++; continue; }

      const employeeNameCanonical = commaSwapName(employeeNameRaw);

      // --- Mitarbeiter-ID finden (by normalized name, inkl. Komma-Umkehr) ---
      const n1 = normalizeName(employeeNameRaw);
      const n2 = normalizeName(employeeNameCanonical);

      let emp = await pool.query(
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

        await pool.query(
          `
          INSERT INTO employees (employee_id, name)
          VALUES ($1, $2)
          ON CONFLICT (employee_id) DO UPDATE SET name = EXCLUDED.name
          `,
          [employeeId, employeeNameCanonical]
        );
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

        await pool.query(
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
      }
    }

    return res.json({
      ok: true,
      imported,
      skipped_no_employee_rows: skippedNoEmployee,
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
// TIME / BREAKS
// ======================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  try {
    const employeeId = String(req.params.employee_id || "").trim();
    const r = await pool.query(
      `
      SELECT id, start_time
      FROM time_entries
      WHERE employee_id=$1 AND end_time IS NULL
      ORDER BY id DESC
      LIMIT 1
      `,
      [employeeId]
    );
    if (!r.rowCount) return res.json({ ok: false });
    res.json({ ok: true, start_time: r.rows[0].start_time });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const today = new Date().toISOString().slice(0, 10);

    const open = await pool.query(
      `SELECT id FROM time_entries WHERE employee_id=$1 AND end_time IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) {
      const row = await pool.query(`SELECT start_time FROM time_entries WHERE id=$1`, [open.rows[0].id]);
      return res.json({ ok: true, start_time: row.rows[0].start_time, note: "already_running" });
    }

    const { rows: projects } = await getTodayProjectsSmart(employeeId, today);
    const picked = projects && projects.length ? projects[0] : null;

    const now = new Date().toISOString();

    const ins = await pool.query(
      `
      INSERT INTO time_entries
        (employee_id, work_date, customer_po, internal_po, project_short, requester_name, start_time, start_ts, activity, break_minutes)
      VALUES
        ($1, $2::date, $3, $4, $5, $6, $7::timestamptz, $7::timestamptz, $8, 0)
      RETURNING start_time
      `,
      [
        employeeId,
        today,
        picked ? picked.customer_po : null,
        picked ? picked.internal_po : null,
        picked ? picked.project_short : null,
        picked ? picked.requester_name : null,
        now,
        "Arbeitszeit",
      ]
    );

    return res.json({
      ok: true,
      start_time: ins.rows[0].start_time,
      picked_project: picked
        ? {
            customer_po: picked.customer_po,
            internal_po: picked.internal_po,
            project_short: picked.project_short,
            requester_name: picked.requester_name,
          }
        : null,
    });

  } catch (e) {
    console.error("TIME START ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    const activity = String(req.body.activity || "Arbeitszeit").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const open = await pool.query(
      `SELECT id, work_date, start_time FROM time_entries WHERE employee_id=$1 AND end_time IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Kein offener Arbeitsblock" });

    const row = open.rows[0];
    const now = new Date();

    const br = await pool.query(
      `
      SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (end_ts - start_ts)))/60,0)::int AS mins
      FROM breaks
      WHERE employee_id=$1 AND work_date=$2::date AND end_ts IS NOT NULL
      `,
      [employeeId, row.work_date]
    );
    const breakMinutes = br.rows[0].mins || 0;

    const start = new Date(row.start_time);
    const grossHours = Math.max(0, (now.getTime() - start.getTime()) / 3600000);
    const netHours = Math.max(0, grossHours - breakMinutes / 60);

    await pool.query(
      `
      UPDATE time_entries
      SET end_time=$1::timestamptz,
          end_ts=$1::timestamptz,
          activity=$2,
          break_minutes=$3
      WHERE id=$4
      `,
      [now.toISOString(), activity, breakMinutes, row.id]
    );

    return res.json({
      ok: true,
      net_hours: netHours.toFixed(2),
      break_minutes: breakMinutes,
    });

  } catch (e) {
    console.error("TIME END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/break/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const today = new Date().toISOString().slice(0, 10);

    const open = await pool.query(
      `SELECT id FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (open.rowCount) return res.json({ ok: true, note: "already_running" });

    const now = new Date().toISOString();
    await pool.query(
      `INSERT INTO breaks (employee_id, work_date, start_ts) VALUES ($1, $2::date, $3::timestamptz)`,
      [employeeId, today, now]
    );

    res.json({ ok: true });
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
      `SELECT id, start_ts FROM breaks WHERE employee_id=$1 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [employeeId]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Keine offene Pause" });

    const now = new Date().toISOString();
    await pool.query(`UPDATE breaks SET end_ts=$1::timestamptz WHERE id=$2`, [now, open.rows[0].id]);

    const mins = Math.max(
      0,
      Math.round((new Date(now).getTime() - new Date(open.rows[0].start_ts).getTime()) / 60000)
    );

    res.json({ ok: true, minutes: mins });
  } catch (e) {
    console.error("BREAK END ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// DEBUG: Staffplan
// ======================================================
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

  const q = employeeId
    ? `
      SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
      FROM staffplan
      WHERE work_date=$1::date AND employee_id=$2
      ORDER BY employee_id, customer_po, internal_po
      `
    : `
      SELECT employee_id, employee_name, requester_name, customer_po, internal_po, project_short, planned_hours
      FROM staffplan
      WHERE work_date=$1::date
      ORDER BY employee_id, customer_po, internal_po
      `;

  const r = employeeId ? await pool.query(q, [date, employeeId]) : await pool.query(q, [date]);
  res.json({ ok: true, date, rows: r.rows });
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
// DEBUG: Time
// ======================================================
app.get("/api/debug/time-rows", async (req, res) => {
  const employeeId = String(req.query.employee_id || "").trim();
  const r = employeeId
    ? await pool.query(
        `
        SELECT id, work_date, customer_po, internal_po, project_short, requester_name, start_time, end_time, activity
        FROM time_entries
        WHERE employee_id=$1
        ORDER BY id DESC
        LIMIT 50
        `,
        [employeeId]
      )
    : await pool.query(
        `
        SELECT id, employee_id, work_date, customer_po, internal_po, project_short, requester_name, start_time, end_time, activity
        FROM time_entries
        ORDER BY id DESC
        LIMIT 50
        `
      );

  res.json({ ok: true, rows: r.rows });
});

app.post("/api/debug/cleanup-time", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const del = await pool.query(
      `DELETE FROM time_entries WHERE employee_id=$1 AND (start_time IS NULL) RETURNING id`,
      [employeeId]
    );

    const close = await pool.query(
      `
      UPDATE time_entries
      SET end_time = COALESCE(end_time, NOW()),
          end_ts = COALESCE(end_ts, NOW())
      WHERE employee_id=$1 AND end_time IS NULL AND start_time IS NOT NULL
      RETURNING id
      `,
      [employeeId]
    );

    const closeBreaks = await pool.query(
      `
      UPDATE breaks
      SET end_ts = COALESCE(end_ts, NOW())
      WHERE employee_id=$1 AND end_ts IS NULL
      RETURNING id
      `,
      [employeeId]
    );

    return res.json({
      ok: true,
      employee_id: employeeId,
      deleted_null_start: del.rowCount,
      closed_open_time_entries: close.rowCount,
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
