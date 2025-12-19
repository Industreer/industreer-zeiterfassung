// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (NUMERIC FIX)
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
// PDF – STUNDENNACHWEIS (FINAL STABIL)
// ============================================================
app.get("/api/pdf/timesheet/:employeeId/:kw/:customerPo/:internalPo", async (req, res) => {
  const { employeeId, kw, customerPo, internalPo } = req.params;

  const emp = await pool.query(
    "SELECT name FROM employees WHERE employee_id=$1",
    [employeeId]
  );
  if (!emp.rows.length) return res.sendStatus(404);

  const entries = await pool.query(
    `SELECT work_date, total_hours
     FROM time_entries
     WHERE employee_id=$1
     ORDER BY work_date`,
    [employeeId]
  );

  const doc = new PDFDocument({ size: "A4", margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  // ===== LOGO
  if (fs.existsSync(LOGO_FILE)) {
    const buffer = fs.readFileSync(LOGO_FILE);
    const meta = JSON.parse(fs.readFileSync(LOGO_META));
    const format = meta.mimetype === "image/png" ? "PNG" : "JPEG";
    const w = 200;
    doc.image(buffer, (doc.page.width - w) / 2, 25, { width: w, format });
  }

  // ===== HEADER
  doc.font("Helvetica-Bold").fontSize(16)
     .text("STUNDENNACHWEIS", 0, 110, { align: "center" });

  doc.font("Helvetica").fontSize(9);
  doc.text(`Mitarbeiter: ${emp.rows[0].name}`, 40, 140);
  doc.text(`Kalenderwoche: ${kw}`, 40, 155);
  doc.text(`Kunden-PO: ${customerPo}`, 300, 140);
  doc.text(`Interne PO: ${internalPo}`, 300, 155);

  // ===== TABELLE
  let y = 185;
  const rowH = 14;

  doc.font("Helvetica-Bold");
  doc.text("Datum", 40, y);
  doc.text("Tätigkeit", 200, y);
  doc.text("Std.", 500, y, { align: "right" });
  y += rowH;

  doc.moveTo(40, y).lineTo(550, y).stroke();
  y += 4;

  doc.font("Helvetica");
  let sum = 0;

  entries.rows.forEach(r => {
    const hours = Number(r.total_hours || 0);
    sum += hours;

    doc.text(new Date(r.work_date).toLocaleDateString("de-DE"), 40, y);
    doc.text("Arbeitszeit", 200, y);
    doc.text(hours.toFixed(2), 500, y, { align: "right" });

    y += rowH;
  });

  // ===== SUMME
  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Gesamtstunden:", 350, y);
  doc.text(sum.toFixed(2), 500, y, { align: "right" });

  // ===== UNTERSCHRIFTEN
  y += 40;
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
app.listen(PORT, () => {
  console.log("🚀 Server läuft auf Port", PORT);
});
