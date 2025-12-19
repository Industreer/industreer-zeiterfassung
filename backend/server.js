// ======================================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (MULTI START/STOP + BREAKS PER BLOCK)
// - /admin, /employee
// - Logo Upload + /api/logo
// - Staffplan Import (tolerant)
// - Employee lookup: /api/employee/:id
// - Arbeitsblöcke pro Tag: mehrere Start/Stop möglich
// - Raucherpausen pro Arbeitsblock
// - PDF Timesheet: Start/Ende + Pause + Netto pro Block
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

// -------------------- db --------------------
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// -------------------- migrate --------------------
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
      total_hours NUMERIC(8,2),
      activity TEXT
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

  // add time_entry_id if older table exists without it
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
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_name TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS customer_po TEXT;`);
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
}

// -------------------- upload --------------------
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 25 * 1024 * 1024 }
});

// -------------------- logo --------------------
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });
  if (!["image/png", "image/jpeg"].includes(req.file.mimetype)) {
    return res.status(400).json({ ok: false, error: "Nur PNG oder JPG" });
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
  const r = await pool.query(
    "SELECT employee_id, name, email, language FROM employees WHERE employee_id=$1",
    [id]
  );
  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });
  res.json({ ok: true, employee: r.rows[0] });
});

// -------------------- helpers: date/hours tolerant --------------------
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

// -------------------- staffplan import (tolerant) --------------------
app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Keine Datei" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const ws = wb.Sheets[wb.SheetNames[0]];
    const calendarWeek = ws["L2"] ? String(ws["L2"].v).trim() : null;

    // find date row starting col L
    let dates = [];
    for (let r = 1; r <= 15; r++) {
      const found = [];
      for (let c = 11; c < 60; c++) {
        const cell = ws[XLSX.utils.encode_cell({ r: r - 1, c })];
        const iso = cell ? parseDateAny(cell.v) : null;
        if (iso) found.push({ c, iso });
      }
      if (found.length >= 3) {
        dates = found;
        break;
      }
    }

    if (!dates.length) return res.json({ ok: true, calendarWeek, imported: 0 });

    if (calendarWeek) {
      await pool.query("DELETE FROM staffplan WHERE calendar_week=$1", [calendarWeek]);
    }

    let imported = 0;

    for (let row = 6; row < 6000; row++) {
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
// MULTI START/STOP PER DAY
// ======================================================================

// Create a NEW work block each time you press start
app.post("/api/time/start", async (req, res) => {
  const employee_id = String(req.body.employee_id || "").trim();
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  // optional: prevent double-start if there's already an open block
  const open = await pool.query(
    `SELECT id FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (open.rows.length) {
    return res.status(400).json({ ok: false, error: "Es läuft bereits ein Arbeitsblock" });
  }

  const ins = await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())
     RETURNING id, start_time`,
    [employee_id, today]
  );

  res.json({ ok: true, time_entry_id: ins.rows[0].id, start_time: ins.rows[0].start_time });
});

// End the CURRENT open work block
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

  // sum breaks only for THIS block
  const breaks = await pool.query(
    `SELECT start_time, end_time FROM break_entries
     WHERE time_entry_id=$1 AND end_time IS NOT NULL`,
    [entry.id]
  );

  let breakMs = 0;
  for (const b of breaks.rows) {
    breakMs += new Date(b.end_time) - new Date(b.start_time);
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
    break_minutes: Math.round(breakMs / 60000)
  });
});

// ======================================================================
// BREAKS (per current open work block)
// ======================================================================

app.post("/api/break/start", async (req, res) => {
  const employee_id = String(req.body.employee_id || "").trim();
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const today = new Date().toISOString().slice(0, 10);

  // Must have an open work block
  const open = await pool.query(
    `SELECT id FROM time_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );
  if (!open.rows.length) return res.status(400).json({ ok: false, error: "Kein laufender Arbeitsblock" });

  // Prevent double break
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
    `SELECT * FROM break_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [employee_id, today]
  );

  if (!r.rows.length) return res.status(400).json({ ok: false, error: "Keine aktive Pause" });

  await pool.query(`UPDATE break_entries SET end_time=NOW() WHERE id=$1`, [r.rows[0].id]);

  const mins = Math.round((new Date() - new Date(r.rows[0].start_time)) / 60000);
  res.json({ ok: true, minutes: mins });
});

// ======================================================================
// PDF Timesheet (rows per work block)
// ======================================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const emp = await pool.query("SELECT name FROM employees WHERE employee_id=$1", [employeeId]);
  if (!emp.rows.length) return res.sendStatus(404);
  const employeeName = emp.rows[0].name;

  const head = await pool.query(
    `SELECT customer_name, internal_po
     FROM staffplan
     WHERE calendar_week=$1 AND customer_po=$2 AND employee_name=$3
     ORDER BY work_date
     LIMIT 1`,
    [kw, customerPo, employeeName]
  );

  const customerName = head.rows[0]?.customer_name || "-";
  const internalPo = head.rows[0]?.internal_po || "-";

  // all work blocks for this employee (you can later filter by week dates if you want)
  const entries = await pool.query(
    `SELECT id, work_date, start_time, end_time, total_hours, activity
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date, start_time`,
    [employeeId]
  );

  // breaks grouped by time_entry_id
  const breaks = await pool.query(
    `SELECT time_entry_id, start_time, end_time
     FROM break_entries
     WHERE employee_id=$1 AND end_time IS NOT NULL`,
    [employeeId]
  );

  const breakMinutesByEntry = new Map();
  for (const b of breaks.rows) {
    const id = b.time_entry_id;
    if (!id) continue;
    const mins = Math.round((new Date(b.end_time) - new Date(b.start_time)) / 60000);
    breakMinutesByEntry.set(id, (breakMinutesByEntry.get(id) || 0) + mins);
  }

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // logo centered
  if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
    const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
    const w = 220;
    doc.image(fs.readFileSync(LOGO_FILE), (doc.page.width - w) / 2, 25, { width: w, format });
  }

  doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });
  doc.font("Helvetica").fontSize(9);

  doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`Kalenderwoche: ${kw}`, 40, 170);

  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

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

  for (const r of entries.rows) {
    if (!r.end_time) continue; // only completed blocks
    const h = Number(r.total_hours || 0);
    if (!h) continue;

    const bm = breakMinutesByEntry.get(r.id) || 0;

    sum += h;

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

  y += 8;
  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 380, y);
  doc.text(sum.toFixed(2), 520, y, { align: "right" });

  doc.end();
});

// -------------------- start --------------------
migrate().then(() => {
  app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
});
