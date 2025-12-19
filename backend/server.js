// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (LOGO + STAFFPLAN IMPORT)
// ============================================================

const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ============================================================
// MIDDLEWARE
// ============================================================
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ============================================================
// PAGES
// ============================================================
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);
app.get("/admin", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"))
);

// ============================================================
// DATABASE
// ============================================================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ============================================================
// MIGRATION
// ============================================================
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
      employee_id TEXT,
      work_date DATE,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      total_hours NUMERIC(6,2),
      activity TEXT DEFAULT 'Arbeitszeit'
    );
  `);

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
  await pool.query(`ALTER TABLE staffplan ADD COLUMN IF NOT EXISTS internal_po TEXT;`);
}

// ============================================================
// UPLOADS (multer memory)
// ============================================================
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 15 * 1024 * 1024 } });

// ============================================================
// LOGO STORAGE (format-safe for PDF)
// ============================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: "No file" });
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

// ============================================================
// HEALTH
// ============================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ============================================================
// STAFFPLAN IMPORT
// Erwartung laut dir:
// - Datum in Zeile 4 ab Zelle L4 (also Row 4, Col 12 -> 0-based: row=3, col=11)
// - KW in Zelle L2 (row=1, col=11)
// - Customer name in Spalte A ab Zeile 6
// - Interne PO in Spalte B ab Zeile 6
// - Kunden-PO in Spalte E
// - Mitarbeitername in Spalte I (in 2er Schritten, aber wir lesen einfach jede Zeile ab 6)
// - Tagesstunden stehen in den Datums-Spalten (ab L) in der gleichen Zeile
// ============================================================
function excelDateToISO(v) {
  // v kann JS Date, Excel Serial, oder String sein
  if (v instanceof Date && !isNaN(v)) return v.toISOString().slice(0, 10);

  if (typeof v === "number") {
    // Excel serial date (days since 1899-12-30)
    const ms = Math.round((v - 25569) * 86400 * 1000);
    const d = new Date(ms);
    if (!isNaN(d)) return d.toISOString().slice(0, 10);
  }

  if (typeof v === "string") {
    // akzeptiere "YYYY-MM-DD" oder "DD.MM.YYYY"
    const s = v.trim();
    const isoMatch = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (isoMatch) return s;

    const deMatch = s.match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
    if (deMatch) {
      const dd = String(deMatch[1]).padStart(2, "0");
      const mm = String(deMatch[2]).padStart(2, "0");
      const yyyy = deMatch[3];
      return `${yyyy}-${mm}-${dd}`;
    }
  }

  return null;
}

app.post("/api/import/staffplan", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) return res.status(400).json({ ok: false, error: "Bitte Excel-Datei auswählen" });

    const wb = XLSX.read(req.file.buffer, { type: "buffer" });
    const sheetName = wb.SheetNames[0];
    const ws = wb.Sheets[sheetName];

    // L2 = KW
    const kwCell = ws["L2"] ? ws["L2"].v : null;
    const calendarWeek = kwCell ? String(kwCell).trim() : null;

    // Row 4 ab L4: Datumsheader
    // Wir lesen solange Zellen existieren.
    const dates = []; // [{col: number(0-based), iso: 'YYYY-MM-DD', label:'...'}]
    // L = 12 (1-based), also 0-based col=11
    const startCol0 = 11;
    const headerRow = 4; // 1-based
    for (let c0 = startCol0; c0 < startCol0 + 60; c0++) {
      const addr = XLSX.utils.encode_cell({ r: headerRow - 1, c: c0 });
      const cell = ws[addr];
      if (!cell) break;

      const iso = excelDateToISO(cell.v);
      if (!iso) break;

      dates.push({ c0, iso });
    }

    if (!dates.length) {
      return res.status(400).json({
        ok: false,
        error: "Keine Datumszeile gefunden. Erwartet Datum ab Zelle L4."
      });
    }

    // optional: vorherige KW löschen (falls du immer neu importierst)
    if (calendarWeek) {
      await pool.query("DELETE FROM staffplan WHERE calendar_week=$1", [calendarWeek]);
    }

    let imported = 0;

    // Ab Zeile 6: Daten
    for (let row = 6; row < 10000; row++) {
      const customerNameCell = ws[XLSX.utils.encode_cell({ r: row - 1, c: 0 })]; // A
      const internalPoCell = ws[XLSX.utils.encode_cell({ r: row - 1, c: 1 })];   // B
      const customerPoCell  = ws[XLSX.utils.encode_cell({ r: row - 1, c: 4 })];   // E
      const employeeCell    = ws[XLSX.utils.encode_cell({ r: row - 1, c: 8 })];   // I

      const customer_name = customerNameCell ? String(customerNameCell.v).trim() : "";
      const internal_po = internalPoCell ? String(internalPoCell.v).trim() : "";
      const customer_po = customerPoCell ? String(customerPoCell.v).trim() : "";
      const employee_name = employeeCell ? String(employeeCell.v).trim() : "";

      // Abbruchbedingung: wenn komplett leer, dann Ende
      if (!customer_name && !internal_po && !customer_po && !employee_name) break;

      // Nur Zeilen importieren, die einen Mitarbeiter haben
      if (!employee_name) continue;

      // Für jeden Tag: wenn in der Zelle Stunden stehen → Eintrag erzeugen
      for (const d of dates) {
        const cellAddr = XLSX.utils.encode_cell({ r: row - 1, c: d.c0 });
        const hoursCell = ws[cellAddr];
        if (!hoursCell) continue;

        const hours = Number(hoursCell.v);
        if (!isFinite(hours) || hours <= 0) continue;

        await pool.query(
          `INSERT INTO staffplan (calendar_week, employee_name, work_date, customer_name, customer_po, internal_po)
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [calendarWeek, employee_name, d.iso, customer_name || null, customer_po || null, internal_po || null]
        );

        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, imported });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  }
});

// ============================================================
// PDF TIMESHEET
// - Interne PO + Kunde einmal im Kopf
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo", async (req, res) => {
  const { employeeId, kw, customerPo } = req.params;

  const empRes = await pool.query("SELECT name FROM employees WHERE employee_id=$1", [employeeId]);
  if (!empRes.rows.length) return res.sendStatus(404);
  const employeeName = empRes.rows[0].name;

  // Kopf aus Staffplan: 1 Datensatz für die KW + KundenPO
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

  const entriesRes = await pool.query(
    `SELECT work_date, total_hours, activity
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date`,
    [employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // Logo mittig
  if (fs.existsSync(LOGO_FILE) && fs.existsSync(LOGO_META)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META, "utf8"));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
    const w = 200;
    doc.image(buffer, (doc.page.width - w) / 2, 25, { width: w, format });
  }

  doc.font("Helvetica-Bold").fontSize(16).text("STUNDENNACHWEIS", 0, 110, { align: "center" });

  doc.font("Helvetica").fontSize(9);
  doc.text(`Mitarbeiter: ${employeeName}`, 40, 140);
  doc.text(`Kunde: ${customerName}`, 40, 155);
  doc.text(`Kalenderwoche: ${kw}`, 40, 170);

  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  let y = 195;
  const rowH = 13;

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Tätigkeit", 200, y);
  doc.text("Std.", 520, y, { align: "right" });
  y += rowH + 3;
  doc.moveTo(40, y).lineTo(550, y).stroke();
  y += 4;

  doc.font("Helvetica");
  let sum = 0;

  for (const r of entriesRes.rows) {
    const h = Number(r.total_hours || 0);
    if (!h) continue;
    sum += h;

    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text(r.activity || "Arbeitszeit", 200, y);
    doc.text(h.toFixed(2), 520, y, { align: "right" });
    y += rowH;
  }

  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Gesamt:", 380, y);
  doc.text(sum.toFixed(2), 520, y, { align: "right" });

  y += 35;
  doc.font("Helvetica");
  doc.text("Unterschrift Mitarbeiter:", 40, y);
  doc.text("__________________________", 40, y + 12);
  doc.text("Unterschrift Kunde:", 300, y);
  doc.text("__________________________", 300, y + 12);

  doc.end();
});

// ============================================================
// START
// ============================================================
migrate().then(() => {
  app.listen(PORT, () => console.log("🚀 Server läuft auf Port", PORT));
});
