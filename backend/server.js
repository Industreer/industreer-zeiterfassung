// ======================================================================
// INDUSTREER ZEITERFASSUNG – server.js (FULL)
// - /admin + /employee
// - Logo Upload + /api/logo
// - Staffplan Import (Excel tolerant)
// - Employee Lookup /api/employee/:id
// - Current running work block /api/time/current/:employee_id (Timer-Fix)
// - Multi Start/Stop per day (Work-Blocks)
// - Smoking breaks per Work-Block (Nettozeit)
// - Option B: POs pro Mitarbeiter/KW /api/employee/:employeeId/pos/:kw
// - PDF Timesheet /api/pdf/timesheet/:employeeId/:kw/:customerPo
//     -> Filtert ausschließlich Staffplan-Tage (KW+PO+Mitarbeiter)
// ======================================================================

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

// -------------------- migrations --------------------
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
      internal_po TEXT
    );
  `);

  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS calendar_week TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS employee_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS work_date DATE;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
}

// -------------------- upload --------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 },
});

// -------------------- logo --------------------
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

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

// -------------------- health --------------------
app.get("/api/health", (_, res) => res.json({ ok: true }));

// -------------------- employee lookup --------------------
app.get("/api/employee/:id", async (req, res) => {
  const id = String(req.params.id || "").trim();
  if (!id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1",
    [id]
  );
  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

  res.json({ ok: true, employee: r.rows[0] });
});

// ======================================================================
// FIX #1: Current running work block (reliable timer after reload)
// ======================================================================
app.get("/api/time/current/:employee_id", async (req, res) => {
  const employee_id = String(req.params.employee_id || "").trim();
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
// OPTION B: Alle POs eines Mitarbeiters für eine KW (aus Staffplan)
// ======================================================================
app.get("/api/employee/:employeeId/pos/:kw", async (req, res) => {
  try {
    const { employeeId, kw } = req.params;

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

// ===================== tolerant helpers =====================
function parseDateAny(v) {
  if (!v) return null;

  if (v instanceof Date && !isNaN(v)) return v.toISOString().slice(0, 10);

  if (typeof v === "number") {
    const d = new Date(Math.round((v - 25569) * 86400 * 1000));
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  if (typeof v === "string") {
    const m = v.match(/(\d{1,2})\.(\d{1,2})\.(\d{4})/);
    if (m) return `${m[3]}-${m[2].padStart(2, "0")}-${m[1].padStart(2, "0")}`;
  }

  return null;
}

function parseHoursAny(v) {
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && isFinite(v)) return v;

  if (typeof v === "string") {
    const m = v.replace(",", ".").match(/(\d+(\.\d+)?)/);
    if (m) return Number(m[1]);
  }
  return null;
}

// ===================== staffplan import (tolerant) =====================
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];

    const calendarWeek = ws["L2"] ? String(ws["L2"].v).trim() : null;

    // Find date header row (starting at column L = index 11)
    let dates = [];
    for (let r = 1; r <= 20; r++) {
      const found = [];
      for (let c = 11; c < 80; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: r - 1, c })];
        const iso = cell ? parseDateAny(cell.v) : null;
        if (iso) found.push({ c, iso });
      }
      if (found.length >= 3) {
        dates = found;
        break;
      }
    }

    if (!dates.length) {
      return res.json({ ok: true, calendarWeek, imported: 0 });
    }

    if (calendarWeek) {
      await pool.query("DELETE FROM staffplan WHERE calendar_week=$1", [calendarWeek]);
    }

    let imported = 0;

    for (let row = 6; row < 7000; row++) {
      const customer_name = ws[`A${row}`]?.v?.toString().trim() || "";
      const internal_po = ws[`B${row}`]?.v?.toString().trim() || "";
      const customer_po = ws[`E${row}`]?.v?.toString().trim() || "";
      const employee_name = ws[`I${row}`]?.v?.toString().trim() || "";

      if (!employee_name) continue;

      for (const d of dates) {
        const cell = ws[XLSX.utils.encode_cell({ r: row - 1, c: d.c })];
        const hours = parseHoursAny(cell?.v);
        if (!hours || hours <= 0) continue;

        await pool.query(
          `INSERT INTO staffplan (calendar_week, employee_name, work_date, customer_name, customer_po, internal_po)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [calendarWeek, employee_name, d.iso, customer_name, customer_po, internal_po]
        );
        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, imported });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ======================================================================
// Multi start/stop per day (work blocks) + breaks per block
// ======================================================================
app.post("/api/time/start", async (req, res) => {
  const employee_id = String(req.body.employee_id || "").trim();
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  // prevent double-start
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
  const employee_id = String(req.body.employee_id || "").trim();
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
  const employee_id = String(req.body.employee_id || "").trim();
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
  const employee_id = String(req.body.employee_id || "").trim();
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const activity = String(req.body.activity || "Arbeitszeit").trim();
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
// PDF Timesheet (filter by staffplan days KW+PO+Mitarbeiter)
// Route: /api/pdf/timesheet/:employeeId/:kw/:customerPo
// ======================================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  try {
    const { employeeId, kw, customerPo } = req.params;

    // employee name (match staffplan.employee_name)
    const emp = await pool.query("SELECT name FROM employees WHERE employee_id=$1", [employeeId]);
    if (!emp.rows.length) return res.sendStatus(404);
    const employeeName = emp.rows[0].name;

    // staffplan days for this KW+PO+employee
    const sp = await pool.query(
      `SELECT DISTINCT work_date, customer_name, internal_po
       FROM staffplan
       WHERE calendar_week=$1 AND customer_po=$2 AND employee_name=$3
       ORDER BY work_date`,
      [kw, customerPo, employeeName]
    );

    if (!sp.rows.length) {
      return res.status(404).send("Keine Staffplan-Daten für diese KW/PO/Mitarbeiter gefunden.");
    }

    const workDates = sp.rows.map(r => r.work_date);
    const customerName = sp.rows[0].customer_name || "-";
    const internalPo = sp.rows[0].internal_po || "-";

    // time entries only on these staffplan dates (completed blocks)
    const te = await pool.query(
      `SELECT id, work_date, start_time, end_time, total_hours, activity
       FROM time_entries
       WHERE employee_id=$1
         AND work_date = ANY($2::date[])
         AND end_time IS NOT NULL
       ORDER BY work_date, start_time`,
      [employeeId, workDates]
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

    doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
    doc.text(`Interne PO: ${internalPo}`, 300, 155);

    // Table (compact)
    let y = 195;
    const rowH = 12;

    doc.font("Helvetica-Bold");
    doc.text("Datum", 40, y);
    doc.text("Start", 98, y);
    doc.text("Ende", 145, y);
    doc.text("Tätigkeit", 195, y);
    doc.text("Pause", 455, y, { width: 45, align: "right" });
    doc.text("Std.", 520, y, { align: "right" });

    y += rowH + 3;
    doc.moveTo(40, y).lineTo(550, y).stroke();
    y += 4;

    doc.font("Helvetica");
    let sum = 0;
    let sumBreakMins = 0;

    for (const r of te.rows) {
      const h = Number(r.total_hours || 0);
      if (!h) continue;

      const bm = breakMinutesByEntry.get(r.id) || 0;
      sum += h;
      sumBreakMins += bm;

      const d = new Date(r.work_date).toLocaleDateString("de-DE");
      const st = new Date(r.start_time).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
      const et = new Date(r.end_time).toLocaleTimeString("de-DE", { hour: "2-digit", minute: "2-digit" });
      const act = (r.activity || "Arbeitszeit").toString().slice(0, 35);

      doc.text(d, 40, y);
      doc.text(st, 98, y);
      doc.text(et, 145, y);
      doc.text(act, 195, y);
      doc.text(`${bm}m`, 455, y, { width: 45, align: "right" });
      doc.text(h.toFixed(2), 520, y, { align: "right" });

      y += rowH;
      if (y > 760) {
        doc.addPage();
        y = 40;
      }
    }

    y += 10;
    doc.font("Helvetica-Bold");
    doc.text("Gesamt (Netto):", 340, y);
    doc.text(sum.toFixed(2), 520, y, { align: "right" });

    y += 14;
    doc.font("Helvetica");
    doc.text(`Summe Raucherpausen: ${sumBreakMins} Minuten`, 40, y);

    doc.end();
  } catch (e) {
    console.error(e);
    res.status(500).send("PDF Fehler: " + e.message);
  }
});

// -------------------- start --------------------
migrate()
  .then(() => {
    app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
  })
  .catch((e) => {
    console.error("DB migrate failed:", e);
    process.exit(1);
  });
