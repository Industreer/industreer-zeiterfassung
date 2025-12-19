// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (DYNAMIC ACTIVITY)
// ============================================================

const express = require("express");
const path = require("path");
const fs = require("fs");
const multer = require("multer");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ============================================================
// MIDDLEWARE
// ============================================================
app.use(express.json());
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
// MIGRATION (inkl. Tätigkeit)
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

  // Falls Tabelle schon existiert → Tätigkeit ergänzen
  await pool.query(`
    ALTER TABLE time_entries
    ADD COLUMN IF NOT EXISTS activity TEXT DEFAULT 'Arbeitszeit';
  `);
}

// ============================================================
// LOGO STORAGE
// ============================================================
const LOGO_FILE = path.join(__dirname, "logo.bin");
const LOGO_META = path.join(__dirname, "logo.json");

const upload = multer({ storage: multer.memoryStorage() });

app.post("/api/admin/logo", upload.single("logo"), (req, res) => {
  fs.writeFileSync(LOGO_FILE, req.file.buffer);
  fs.writeFileSync(LOGO_META, JSON.stringify({ mimetype: req.file.mimetype }));
  res.json({ ok: true });
});

app.get("/api/logo", (_, res) => {
  if (!fs.existsSync(LOGO_FILE)) return res.sendStatus(404);
  const meta = JSON.parse(fs.readFileSync(LOGO_META));
  res.setHeader("Content-Type", meta.mimetype);
  res.send(fs.readFileSync(LOGO_FILE));
});

// ============================================================
// HEALTH
// ============================================================
app.get("/api/health", (_, res) => res.json({ ok: true }));

// ============================================================
// ZEITERFASSUNG
// ============================================================
app.post("/api/time/start", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())`,
    [req.body.employee_id, today]
  );
  res.json({ ok: true });
});

app.post("/api/time/end", async (req, res) => {
  const { employee_id, activity } = req.body;
  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY id DESC LIMIT 1`,
    [employee_id, today]
  );

  if (!r.rows.length) return res.status(400).json({ ok: false });

  const entry = r.rows[0];
  const hours =
    (new Date() - new Date(entry.start_time)) / 36e5;

  await pool.query(
    `UPDATE time_entries
     SET end_time=NOW(),
         total_hours=$1,
         activity=$2
     WHERE id=$3`,
    [hours, activity || "Arbeitszeit", entry.id]
  );

  res.json({ ok: true, hours: hours.toFixed(2) });
});

// ============================================================
// PDF – STUNDENNACHWEIS MIT TÄTIGKEIT
// ============================================================
app.get(
  "/api/pdf/timesheet/:employeeId/:kw/:customerPo/:internalPo",
  async (req, res) => {
    const { employeeId, kw, customerPo, internalPo } = req.params;

    const emp = await pool.query(
      "SELECT name FROM employees WHERE employee_id=$1",
      [employeeId]
    );
    if (!emp.rows.length) return res.sendStatus(404);

    const entries = await pool.query(
      `SELECT work_date, total_hours, activity
       FROM time_entries
       WHERE employee_id=$1
       ORDER BY work_date`,
      [employeeId]
    );

    const doc = new PDFDocument({ size: "A4", margin: 40 });
    res.setHeader("Content-Type", "application/pdf");
    doc.pipe(res);

    // Logo
    if (fs.existsSync(LOGO_FILE)) {
      const buffer = fs.readFileSync(LOGO_FILE);
      const meta = JSON.parse(fs.readFileSync(LOGO_META));
      const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
      const w = 200;
      doc.image(buffer, (doc.page.width - w) / 2, 25, {
        width: w,
        format
      });
    }

    // Header
    doc.font("Helvetica-Bold").fontSize(16)
       .text("STUNDENNACHWEIS", 0, 110, { align: "center" });

    doc.font("Helvetica").fontSize(9);
    doc.text(`Mitarbeiter: ${emp.rows[0].name}`, 40, 140);
    doc.text(`KW: ${kw}`, 40, 155);
    doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
    doc.text(`Interne PO: ${internalPo}`, 300, 155);

    // Tabelle
    let y = 185;
    const rowH = 14;

    doc.font("Helvetica-Bold");
    doc.text("Datum", 40, y);
    doc.text("Tätigkeit", 200, y);
    doc.text("Std.", 500, y, { align: "right" });
    y += rowH + 4;

    doc.font("Helvetica");
    let sum = 0;

    entries.rows.forEach(r => {
      const h = Number(r.total_hours || 0);
      sum += h;

      doc.text(
        new Date(r.work_date).toLocaleDateString("de-DE"),
        40,
        y
      );
      doc.text(r.activity || "Arbeitszeit", 200, y);
      doc.text(h.toFixed(2), 500, y, { align: "right" });
      y += rowH;
    });

    y += 6;
    doc.font("Helvetica-Bold");
    doc.text("Gesamt:", 350, y);
    doc.text(sum.toFixed(2), 500, y, { align: "right" });

    y += 40;
    doc.font("Helvetica");
    doc.text("Unterschrift Mitarbeiter:", 40, y);
    doc.text("__________________________", 40, y + 12);
    doc.text("Unterschrift Kunde:", 300, y);
    doc.text("__________________________", 300, y + 12);

    doc.end();
  }
);

// ============================================================
// START
// ============================================================
migrate().then(() => {
  app.listen(PORT, () => {
    console.log("🚀 Server läuft auf Port", PORT);
  });
});
