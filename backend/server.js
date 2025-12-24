/**
 * backend/server.js (stable)
 */

const path = require("path");
const fs = require("fs");

const express = require("express");
const cors = require("cors");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true }));

const PORT = process.env.PORT || 10000;

// --------------------
// Paths
// --------------------
const ROOT = path.join(__dirname, "..");
const FRONTEND_DIR = path.join(ROOT, "frontend");
const DATA_DIR = path.join(__dirname, "data");
const LOGO_FILE = path.join(DATA_DIR, "logo.png");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// --------------------
// DB
// --------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes("render.com")
    ? { rejectUnauthorized: false }
    : undefined,
});

// --------------------
// Upload
// --------------------
const upload = multer({ storage: multer.memoryStorage() });

// --------------------
// Helpers
// --------------------
function toIsoDate(d) {
  return new Date(d).toISOString().slice(0, 10);
}

function getISOWeek(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  return Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
}

function parseAnyDateFromXlsxCell(cell) {
  if (!cell) return null;

  // Excel serial number
  if (typeof cell.v === "number") {
    const epoch = new Date(1899, 11, 30);
    return new Date(epoch.getTime() + cell.v * 86400000);
  }

  // text
  const text = String(cell.w || cell.v || "").trim();

  // yyyy-mm-dd
  let m = text.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return new Date(+m[1], +m[2] - 1, +m[3]);

  // dd.mm.yyyy
  m = text.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (m) return new Date(+m[3], +m[2] - 1, +m[1]);

  return null;
}

async function migrate() {
  // employees
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT DEFAULT 'de'
    );
  `);

  // staffplan
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staffplan (
      id BIGSERIAL PRIMARY KEY,
      employee_name TEXT NOT NULL,
      work_date DATE NOT NULL,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      internal_po TEXT,
      customer_po TEXT,
      project_short TEXT,
      planned_hours NUMERIC
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS staffplan_idx
    ON staffplan (calendar_week, employee_name);
  `);

  // time entries (work sessions)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL REFERENCES employees(employee_id),
      work_date DATE NOT NULL,
      customer_po TEXT,
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ,
      activity TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS time_entries_idx
    ON time_entries (employee_id, work_date);
  `);

  // breaks
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_breaks (
      id BIGSERIAL PRIMARY KEY,
      employee_id TEXT NOT NULL REFERENCES employees(employee_id),
      work_date DATE NOT NULL,
      kind TEXT NOT NULL DEFAULT 'smoke',
      start_ts TIMESTAMPTZ NOT NULL,
      end_ts TIMESTAMPTZ,
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS time_breaks_idx
    ON time_breaks (employee_id, work_date);
  `);
}

// --------------------
// Static
// --------------------
app.use(express.static(FRONTEND_DIR));

app.get("/", (req, res) => res.redirect("/admin"));
app.get("/admin", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "admin.html")));
app.get("/employee", (req, res) => res.sendFile(path.join(FRONTEND_DIR, "employee.html")));
// ======================================================================
// EMPLOYEE LOGIN
// ======================================================================
app.get("/api/employee/:id", async (req, res) => {
  try {
    const { id } = req.params;

    const r = await pool.query(
      `SELECT employee_id, name, email, language
       FROM employees
       WHERE employee_id = $1`,
      [id]
    );

    if (r.rows.length === 0) {
      return res.status(404).json({
        ok: false,
        error: "Mitarbeiter nicht gefunden"
      });
    }

    res.json({
      ok: true,
      employee: r.rows[0]
    });
  } catch (e) {
    console.error("Employee login error:", e);
    res.status(500).json({
      ok: false,
      error: "Serverfehler"
    });
  }
});

// --------------------
// Health
// --------------------
app.get("/health", (req, res) => res.json({ ok: true }));

// --------------------
// Logo API
// --------------------
app.get("/api/logo", (req, res) => {
  if (!fs.existsSync(LOGO_FILE)) return res.status(404).json({ ok: false, error: "Kein Logo vorhanden" });
  res.setHeader("Content-Type", "image/png");
  fs.createReadStream(LOGO_FILE).pipe(res);
});

app.post("/api/logo", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });
    fs.writeFileSync(LOGO_FILE, req.file.buffer);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Employees API
// --------------------
app.get("/api/employees", async (req, res) => {
  try {
    const r = await pool.query(`SELECT employee_id, name, email, language FROM employees ORDER BY name ASC`);
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/employees/upsert", async (req, res) => {
  try {
    const { employee_id, name, email, language } = req.body || {};
    if (!employee_id || !name) return res.status(400).json({ ok: false, error: "employee_id und name erforderlich" });

    await pool.query(
      `
      INSERT INTO employees (employee_id, name, email, language)
      VALUES ($1,$2,$3,$4)
      ON CONFLICT (employee_id) DO UPDATE
      SET name = EXCLUDED.name,
          email = EXCLUDED.email,
          language = EXCLUDED.language
      `,
      [String(employee_id), String(name), email || null, language || "de"]
    );

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Employee Login
// --------------------
app.post("/api/employee/login", async (req, res) => {
  try {
    const { employee_id } = req.body || {};
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const r = await pool.query(
      `SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1`,
      [String(employee_id)]
    );

    if (r.rowCount === 0) return res.status(404).json({ ok: false, error: "Mitarbeiter-ID nicht gefunden" });
    res.json({ ok: true, employee: r.rows[0] });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Today staffplan for employee
// --------------------
app.get("/api/employee/today", async (req, res) => {
  try {
    const employee_id = String(req.query.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const er = await pool.query(`SELECT name FROM employees WHERE employee_id=$1`, [employee_id]);
    if (er.rowCount === 0) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

    const employee_name = er.rows[0].name;
    const todayIso = new Date().toISOString().slice(0, 10);

    const sp = await pool.query(
      `SELECT employee_name, work_date, calendar_week, customer, internal_po, customer_po, project_short, planned_hours
       FROM staffplan
       WHERE employee_name=$1 AND work_date=$2
       ORDER BY customer_po ASC`,
      [employee_name, todayIso]
    );

    res.json({ ok: true, rows: sp.rows });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Time status
// --------------------
app.get("/api/time/status", async (req, res) => {
  try {
    const employee_id = String(req.query.employee_id || "").trim();
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayIso = new Date().toISOString().slice(0, 10);

    const open = await pool.query(
      `SELECT id, start_ts, end_ts FROM time_entries
       WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL
       ORDER BY id DESC LIMIT 1`,
      [employee_id, todayIso]
    );

    const openBreak = await pool.query(
      `SELECT id, start_ts FROM time_breaks
       WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL
       ORDER BY id DESC LIMIT 1`,
      [employee_id, todayIso]
    );

    const sumBreak = await pool.query(
      `SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (COALESCE(end_ts, NOW()) - start_ts)))/60,0) AS mins
       FROM time_breaks
       WHERE employee_id=$1 AND work_date=$2`,
      [employee_id, todayIso]
    );

    res.json({
      ok: true,
      open: open.rowCount ? open.rows[0] : null,
      break: {
        is_on_break: openBreak.rowCount > 0,
        started_at: openBreak.rowCount ? openBreak.rows[0].start_ts : null,
        total_break_minutes: Math.round(Number(sumBreak.rows[0].mins || 0)),
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Time start
// --------------------
app.post("/api/time/start", async (req, res) => {
  try {
    const { employee_id, customer_po } = req.body || {};
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });
    if (!customer_po) return res.status(400).json({ ok: false, error: "customer_po fehlt" });

    const todayIso = new Date().toISOString().slice(0, 10);

    const existing = await pool.query(
      `SELECT start_ts FROM time_entries
       WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL
       ORDER BY id DESC LIMIT 1`,
      [String(employee_id), todayIso]
    );
    if (existing.rowCount) return res.json({ ok: true, start_ts: existing.rows[0].start_ts, note: "already_started" });

    const ins = await pool.query(
      `INSERT INTO time_entries (employee_id, work_date, customer_po, start_ts)
       VALUES ($1,$2,$3,NOW())
       RETURNING start_ts`,
      [String(employee_id), todayIso, String(customer_po)]
    );

    res.json({ ok: true, start_ts: ins.rows[0].start_ts });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Smoke break start/end
// --------------------
app.post("/api/time/smoke/start", async (req, res) => {
  try {
    const { employee_id } = req.body || {};
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayIso = new Date().toISOString().slice(0, 10);

    const openBreak = await pool.query(
      `SELECT id FROM time_breaks WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL LIMIT 1`,
      [String(employee_id), todayIso]
    );
    if (openBreak.rowCount) return res.json({ ok: true, note: "already_on_break" });

    await pool.query(
      `INSERT INTO time_breaks (employee_id, work_date, kind, start_ts)
       VALUES ($1,$2,'smoke',NOW())`,
      [String(employee_id), todayIso]
    );

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/api/time/smoke/end", async (req, res) => {
  try {
    const { employee_id } = req.body || {};
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayIso = new Date().toISOString().slice(0, 10);

    const openBreak = await pool.query(
      `SELECT id FROM time_breaks WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL ORDER BY id DESC LIMIT 1`,
      [String(employee_id), todayIso]
    );
    if (!openBreak.rowCount) return res.status(400).json({ ok: false, error: "Keine aktive Raucherpause" });

    await pool.query(`UPDATE time_breaks SET end_ts=NOW() WHERE id=$1`, [openBreak.rows[0].id]);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Time stop
// --------------------
app.post("/api/time/stop", async (req, res) => {
  try {
    const { employee_id, customer_po, activity } = req.body || {};
    if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

    const todayIso = new Date().toISOString().slice(0, 10);

    const open = await pool.query(
      `SELECT id FROM time_entries
       WHERE employee_id=$1 AND work_date=$2 AND end_ts IS NULL
       ORDER BY id DESC LIMIT 1`,
      [String(employee_id), todayIso]
    );
    if (!open.rowCount) return res.status(400).json({ ok: false, error: "Kein aktiver Arbeitstag" });

    await pool.query(
      `UPDATE time_entries
       SET end_ts=NOW(),
           customer_po=COALESCE($2, customer_po),
           activity=$3
       WHERE id=$1`,
      [open.rows[0].id, customer_po ? String(customer_po) : null, activity ? String(activity) : null]
    );

    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// Staffplan import
// --------------------
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei hochgeladen" });

    const workbook = XLSX.read(req.file.buffer, { type: "buffer" });
    const sheetName = workbook.SheetNames[0];
    const ws = workbook.Sheets[sheetName];

    const startCol = 11; // L
    const maxRightCols = 1200;
    const maxScanRows = 15;

    // find first date cell
    let baseDate = null;
    let headerColStart = null;

    for (let r = 0; r < maxScanRows; r++) {
      for (let c = startCol; c < startCol + maxRightCols; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r, c })];
        const d = parseAnyDateFromXlsxCell(cell);
        if (d) {
          baseDate = d;
          headerColStart = c;
          break;
        }
      }
      if (baseDate) break;
    }

    if (!baseDate) {
      return res.status(400).json({ ok: false, error: "Kein Startdatum gefunden (Datumszeile ab L4 o.ä.)" });
    }

    // compute dates
    const computedDates = [];
    for (let c = headerColStart; c < startCol + maxRightCols; c++) {
      const d = new Date(baseDate);
      d.setDate(baseDate.getDate() + (c - headerColStart));
      computedDates.push({ col: c, iso: toIsoDate(d), cw: "CW" + getISOWeek(d) });
    }

    await pool.query("DELETE FROM staffplan");

    let imported = 0;
    let minDate = null;
    let maxDate = null;
    const weeks = new Set();

    for (let row = 5; row < 20000; row++) {
      const nameCell = ws[XLSX.utils.encode_cell({ r: row, c: 8 })]; // I
      if (!nameCell?.v) continue;

      const employeeName = String(nameCell.v).trim();
      if (!employeeName) continue;

      const customerCell = ws[XLSX.utils.encode_cell({ r: row, c: 0 })]; // A
      const internalPoCell = ws[XLSX.utils.encode_cell({ r: row, c: 1 })]; // B
      const customerPoCell = ws[XLSX.utils.encode_cell({ r: row, c: 4 })]; // E

      const customer = customerCell?.v ? String(customerCell.v).trim() : null;
      const internalPo = internalPoCell?.v ? String(internalPoCell.v).trim() : null;
      const customerPo = customerPoCell?.v ? String(customerPoCell.v).trim() : null;

      // auto-add employee if missing (name-based)
      const exists = await pool.query(`SELECT 1 FROM employees WHERE name=$1 LIMIT 1`, [employeeName]);
      if (exists.rowCount === 0) {
        const newId = "AUTO" + String(row);
        await pool.query(
          `INSERT INTO employees (employee_id, name) VALUES ($1,$2) ON CONFLICT DO NOTHING`,
          [newId, employeeName]
        );
      }

      for (const d of computedDates) {
        const projectShortCell = ws[XLSX.utils.encode_cell({ r: row, c: d.col })];
        const planHoursCell = ws[XLSX.utils.encode_cell({ r: row + 1, c: d.col })];

        if (!projectShortCell?.v && !planHoursCell?.v) continue;

        const projectShort = projectShortCell?.v ? String(projectShortCell.v).trim() : null;

        let plannedHours = null;
        if (planHoursCell?.v !== undefined && planHoursCell?.v !== null && planHoursCell?.v !== "") {
          plannedHours = Number(planHoursCell.v);
          if (Number.isNaN(plannedHours)) plannedHours = null;
        }

        await pool.query(
          `
          INSERT INTO staffplan
            (employee_name, work_date, calendar_week, customer, internal_po, customer_po, project_short, planned_hours)
          VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8)
          `,
          [employeeName, d.iso, d.cw, customer, internalPo, customerPo, projectShort, plannedHours]
        );

        imported++;
        weeks.add(d.cw);
        if (!minDate || d.iso < minDate) minDate = d.iso;
        if (!maxDate || d.iso > maxDate) maxDate = d.iso;
      }
    }

    res.json({
      ok: true,
      imported,
      dateRange: { from: minDate, to: maxDate },
      weeksDetected: Array.from(weeks).sort(),
    });
  } catch (e) {
    console.error("Staffplan Import Error:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// staffplan sample
// --------------------
app.get("/api/staffplan/sample", async (req, res) => {
  try {
    const r = await pool.query(
      `SELECT employee_name, work_date, calendar_week, customer, internal_po, customer_po, project_short, planned_hours
       FROM staffplan
       ORDER BY work_date ASC
       LIMIT 25`
    );
    res.json(r.rows);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// --------------------
// PDF timesheet (basic)
// --------------------
app.get("/api/timesheet/:employeeName/:kw/:customerPo", async (req, res) => {
  try {
    const employeeName = decodeURIComponent(req.params.employeeName);
    const kw = String(req.params.kw || "");
    const customerPo = decodeURIComponent(req.params.customerPo);

    const sp = await pool.query(
      `SELECT * FROM staffplan
       WHERE employee_name=$1 AND calendar_week=$2 AND customer_po=$3
       ORDER BY work_date ASC`,
      [employeeName, kw, customerPo]
    );

    if (sp.rowCount === 0) return res.status(404).send("Keine Staffplan-Daten gefunden.");

    const first = sp.rows[0];
    const customerName = first.customer || "";
    const internalPo = first.internal_po || "";
    const headerProjectCode = first.project_short || "";

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `inline; filename="Stundennachweis_${kw}_${customerPo}.pdf"`);

    const doc = new PDFDocument({ size: "A4", margin: 40 });
    doc.pipe(res);

    if (fs.existsSync(LOGO_FILE)) {
      try {
        const w = 240;
        doc.image(LOGO_FILE, (doc.page.width - w) / 2, 20, { width: w });
      } catch (_) {}
    }

    doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });

    doc.font("Helvetica").fontSize(9);
    doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
    doc.text(`Kunde: ${customerName}`, 40, 155);
    doc.text(`Kalenderwoche: ${kw}`, 40, 170);

    doc.text(`Projekt (Kurzzeichen): ${headerProjectCode}`, 300, 140);
    doc.text(`Kunden-PO: ${customerPo}`, 300, 155);
    doc.text(`Interne PO: ${internalPo}`, 300, 170);

    let y = 200;
    const rowH = 12;

    doc.font("Helvetica-Bold");
    doc.text("Datum", 40, y);
    doc.text("Plan", 260, y, { width: 40, align: "right" });
    doc.text("IST", 520, y, { align: "right" });

    y += rowH + 3;
    doc.moveTo(40, y).lineTo(550, y).stroke();
    y += 4;
    doc.font("Helvetica");

    let sumPlan = 0;

    for (const r of sp.rows) {
      const dateLabel = new Date(r.work_date).toLocaleDateString("de-DE");
      const plan = Number(r.planned_hours || 0);
      sumPlan += plan;

      doc.text(dateLabel, 40, y);
      doc.text(plan ? plan.toFixed(2) : "", 260, y, { width: 40, align: "right" });
      doc.text("", 520, y, { align: "right" });

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

    doc.end();
  } catch (e) {
    console.error(e);
    res.status(500).send("PDF Fehler: " + e.message);
  }
});

// --------------------
// Start
// --------------------
(async function start() {
  try {
    await migrate();
    app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
  } catch (e) {
    console.error("DB migrate failed:", e);
    process.exit(1);
  }
})();
