/**
 * backend/server.js
 * INDUSTREER Zeiterfassung – robuste Version (Import + Today + Time)
 *
 * Enthält:
 * - Express + Static Frontend (admin.html / employee.html)
 * - Logo Upload
 * - PostgreSQL (Render kompatibel)
 * - Migration (employees + time_entries persistent, staffplan wird beim Start neu erstellt)
 * - Staffplan Import (Excel): erkennt Datumszeile automatisch, liest alle Datumsspalten bis Sheet-Ende
 * - /api/employee/today (mit optionalem ?date=YYYY-MM-DD)
 * - Zeit: /api/time/start /api/time/end /api/time/current/:employeeId
 * - Debug (temporär): /api/debug/staffplan-dates /api/debug/db-info
 */

console.log("🔥 SERVER.JS – INDUSTREER (ROBUST IMPORT + TODAY + TIME) 🔥");

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

  // staffplan IMMER frisch (wie in deinem Original)
  await pool.query(`DROP TABLE IF EXISTS staffplan CASCADE`);

  await pool.query(`
    CREATE TABLE staffplan (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      employee_name TEXT NOT NULL,
      work_date DATE NOT NULL,
      calendar_week TEXT,
      customer TEXT,
      internal_po TEXT,
      customer_po TEXT,
      project_short TEXT,
      planned_hours NUMERIC
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL,
      work_date DATE NOT NULL,
      customer_po TEXT,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ,
      activity TEXT
    );
  `);

  console.log("✅ DB migrate finished");
}

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

/**
 * Robust: Excel-Serial / DD.MM.YYYY / Text mit Datum / DD.MM (heuristisch Jahr)
 */
function parseExcelDate(cell) {
  if (!cell) return null;

  // 1) Excel-Seriennummer (Date)
  if (typeof cell.v === "number" && isFinite(cell.v)) {
    const epoch = new Date(Date.UTC(1899, 11, 30));
    return new Date(epoch.getTime() + cell.v * 86400000);
  }

  // 2) Textdarstellung
  const t = String(cell.w || cell.v || "").trim();
  if (!t) return null;

  // 2a) DD.MM.YYYY
  let m = t.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // 2b) irgendwo enthaltenes DD.MM.YYYY
  m = t.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
  if (m) return new Date(Date.UTC(+m[3], +m[2] - 1, +m[1]));

  // 2c) DD.MM. (ohne Jahr) – Jahr heuristisch um Jahreswechsel stabilisieren
  m = t.match(/^(\d{1,2})\.(\d{1,2})\.$/);
  if (m) {
    const today = new Date();
    const y0 = today.getFullYear();
    let guess = new Date(Date.UTC(y0, +m[2] - 1, +m[1]));

    // Heuristik: wenn > ~200 Tage in der Zukunft, eher Vorjahr; wenn > ~200 Tage in der Vergangenheit, eher Folgejahr
    const diffDays = Math.round((guess.getTime() - today.getTime()) / 86400000);
    if (diffDays > 200) guess = new Date(Date.UTC(y0 - 1, +m[2] - 1, +m[1]));
    if (diffDays < -200) guess = new Date(Date.UTC(y0 + 1, +m[2] - 1, +m[1]));
    return guess;
  }

  // 2d) irgendwo enthaltenes DD.MM. (z. B. "Sa 27.12.")
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

function parsePlannedHours(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && isFinite(v)) return v;

  const s = String(v).trim();
  if (!s) return null;

  // nur reine Zahl erlauben: 8 / 8.5 / 8,5
  if (!/^\d+([.,]\d+)?$/.test(s)) return null;

  const n = parseFloat(s.replace(",", "."));
  return isFinite(n) ? n : null;
}

async function getBerlinTodayISO() {
  const r = await pool.query(`SELECT (now() AT TIME ZONE 'Europe/Berlin')::date AS d`);
  // pg liefert Date manchmal als String, manchmal als Date-Objekt – beides abfangen
  const d = r.rows[0].d;
  return (d instanceof Date) ? d.toISOString().slice(0, 10) : String(d);
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
  const r = await pool.query(`SELECT employee_id,name,email,language FROM employees ORDER BY name`);
  res.json(r.rows);
});

// WICHTIG: /today vor /:id (zur Sicherheit gegen Route-Catch)
app.get("/api/employee/today", async (req, res) => {
  try {
    const employeeId = String(req.query.employee_id || "").trim();
    if (!employeeId) {
      return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    }

    // Optional: ?date=YYYY-MM-DD (für Tests)
    const qDate = String(req.query.date || "").trim();
    const dateISO = qDate || (await getBerlinTodayISO());

    const { rows } = await pool.query(
      `
      SELECT
        work_date,
        calendar_week,
        customer,
        internal_po,
        customer_po,
        project_short,
        planned_hours
      FROM staffplan
      WHERE employee_id = $1
        AND work_date = $2::date
      ORDER BY customer_po, internal_po
      `,
      [employeeId, dateISO]
    );

    return res.json({ ok: true, date: dateISO, projects: rows });
  } catch (e) {
    console.error("EMPLOYEE TODAY ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.get("/api/employee/:id", async (req, res) => {
  const id = String(req.params.id || "").trim();
  const r = await pool.query(
    `SELECT employee_id,name,email,language FROM employees WHERE employee_id=$1`,
    [id]
  );
  if (!r.rowCount) return res.status(404).json({ ok: false, error: "not_found" });
  res.json({ ok: true, employee: r.rows[0] });
});

// ======================================================
// TIME (minimal, damit /api/time/* nicht 404 ist)
// ======================================================
app.get("/api/time/current/:employeeId", async (req, res) => {
  try {
    const employeeId = String(req.params.employeeId || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "missing employeeId" });

    const todayISO = await getBerlinTodayISO();

    const r = await pool.query(
      `
      SELECT id, start_ts
      FROM time_entries
      WHERE employee_id = $1
        AND work_date = $2::date
        AND end_ts IS NULL
      ORDER BY start_ts DESC
      LIMIT 1
      `,
      [employeeId, todayISO]
    );

    if (!r.rowCount) return res.json({ ok: false });

    return res.json({ ok: true, start_time: r.rows[0].start_ts });
  } catch (e) {
    console.error("TIME CURRENT ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/start", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayISO = await getBerlinTodayISO();

    // wenn schon ein offener Block existiert: zurückgeben
    const open = await pool.query(
      `
      SELECT id, start_ts
      FROM time_entries
      WHERE employee_id=$1 AND work_date=$2::date AND end_ts IS NULL
      ORDER BY start_ts DESC
      LIMIT 1
      `,
      [employeeId, todayISO]
    );
    if (open.rowCount) {
      return res.json({ ok: true, start_time: open.rows[0].start_ts, already_running: true });
    }

    const ins = await pool.query(
      `
      INSERT INTO time_entries (employee_id, work_date, customer_po, start_ts)
      VALUES ($1, $2::date, NULL, now())
      RETURNING start_ts
      `,
      [employeeId, todayISO]
    );

    return res.json({ ok: true, start_time: ins.rows[0].start_ts });
  } catch (e) {
    console.error("TIME START ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/end", async (req, res) => {
  try {
    const employeeId = String(req.body.employee_id || "").trim();
    const activity = req.body.activity ? String(req.body.activity).trim() : null;
    if (!employeeId) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayISO = await getBerlinTodayISO();

    const r = await pool.query(
      `
      SELECT id, start_ts
      FROM time_entries
      WHERE employee_id=$1 AND work_date=$2::date AND end_ts IS NULL
      ORDER BY start_ts DESC
      LIMIT 1
      `,
      [employeeId, todayISO]
    );
    if (!r.rowCount) return res.json({ ok: false, error: "Kein laufender Arbeitsblock gefunden" });

    const id = r.rows[0].id;
    const startTs = new Date(r.rows[0].start_ts);

    const upd = await pool.query(
      `
      UPDATE time_entries
      SET end_ts = now(), activity = COALESCE($2, activity)
      WHERE id = $1
      RETURNING end_ts
      `,
      [id, activity]
    );

    const endTs = new Date(upd.rows[0].end_ts);
    const netHours = Math.round(((endTs - startTs) / 3600000) * 100) / 100;

    return res.json({ ok: true, net_hours: netHours, break_minutes: 0 });
  } catch (e) {
    console.error("TIME END ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================
// STAFFPLAN IMPORT (robust)
// ======================================================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const ref = ws["!ref"] || "A1:A1";
    const range = XLSX.utils.decode_range(ref);

    // ----------------------------
    // 1) Header-Zeile automatisch finden (Datumsköpfe)
    //    Wir suchen in den ersten 20 Zeilen nach der Zeile mit den meisten Datumszellen.
    // ----------------------------
    let headerRow = null;
    let headerCountBest = 0;

    const startCol = 11; // ab Spalte L (wie dein Original)
    const endCol = Math.min(range.e.c, 1000);

    for (let r = 0; r <= Math.min(range.e.r, 20); r++) {
      let cnt = 0;
      for (let c = startCol; c <= endCol; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const d = parseExcelDate(cell);
        if (d) cnt++;
      }
      if (cnt > headerCountBest) {
        headerCountBest = cnt;
        headerRow = r;
      }
    }

    if (headerRow === null || headerCountBest < 3) {
      return res.json({
        ok: false,
        error: "Keine brauchbare Datums-Kopfzeile gefunden (Scan 0..20)"
      });
    }

    // ----------------------------
    // 2) Alle Datumsspalten aus der Header-Zeile lesen
    // ----------------------------
    const dates = [];
    for (let c = startCol; c <= endCol; c++) {
      const cell = ws[XLSX.utils.encode_cell({ r: headerRow, c })];
      const d = parseExcelDate(cell);
      if (!d) continue;

      dates.push({
        col: c,
        iso: d.toISOString().slice(0, 10),
        cw: "CW" + getISOWeek(d),
      });
    }

    if (!dates.length) {
      return res.json({ ok: false, error: "Datumszeile gefunden, aber keine Datumszellen parsebar" });
    }

    console.log("📅 HeaderRow:", headerRow + 1, "Dates:", dates[0].iso, "…", dates[dates.length - 1].iso, "count:", dates.length);

    // ----------------------------
    // 3) Staffplan leeren
    // ----------------------------
    await pool.query("DELETE FROM staffplan");

    let imported = 0;

    // ----------------------------
    // 4) Mitarbeiter-Zeilen wie in deinem Original
    //    Start bei r=5, Schritt 2 (r+=2), Name in Spalte I (c=8)
    // ----------------------------
    for (let r = 5; r < 20000; r += 2) {
      const nameCell = ws[XLSX.utils.encode_cell({ r, c: 8 })];
      if (!nameCell?.v) continue;
      const employeeName = String(nameCell.v).trim();

      // Mitarbeiter suchen oder anlegen (Name-Match wie original)
      let emp = await pool.query(
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

      const customer = ws[XLSX.utils.encode_cell({ r, c: 0 })]?.v ?? null;
      const internalPo = ws[XLSX.utils.encode_cell({ r, c: 1 })]?.v ?? null;
      const customerPo = ws[XLSX.utils.encode_cell({ r, c: 4 })]?.v ?? null;

      for (const d of dates) {
        const projRaw = ws[XLSX.utils.encode_cell({ r, c: d.col })]?.v ?? null;
        const planRaw = ws[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]?.v ?? null;

        const proj = projRaw === null || projRaw === undefined || String(projRaw).trim() === "" ? null : String(projRaw).trim();
        const plan = parsePlannedHours(planRaw);

        // wenn beides leer/ungültig -> skip
        if (!proj && plan === null) continue;

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_id, employee_name, work_date, calendar_week,
             customer, internal_po, customer_po, project_short, planned_hours)
          VALUES ($1,$2,$3::date,$4,$5,$6,$7,$8,$9)
          `,
          [
            employeeId,
            employeeName,
            d.iso,
            d.cw,
            customer ? String(customer).trim() : null,
            internalPo ? String(internalPo).trim() : null,
            customerPo ? String(customerPo).trim() : null,
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
// DEBUG (TEMPORARY – nach erfolgreichem Test wieder löschen!)
// ======================================================
app.get("/api/debug/staffplan-dates", async (req, res) => {
  const r = await pool.query(`
    SELECT work_date, COUNT(*)::int AS cnt
    FROM staffplan
    GROUP BY work_date
    ORDER BY work_date DESC
    LIMIT 10
  `);
  res.json(r.rows);
});

app.get("/api/debug/db-info", async (req, res) => {
  const r = await pool.query(`
    SELECT
      current_database() AS db,
      inet_server_addr()::text AS host,
      inet_server_port() AS port,
      now() AS now
  `);
  res.json(r.rows[0]);
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

