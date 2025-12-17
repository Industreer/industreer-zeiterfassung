// ============================================================
// INDUSTREER ZEITERFASSUNG – BACKEND (FINAL + DEBUG)
// - Staffplan Excel Import
// - Debug Route
// - PDF Stundennachweis
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

// ============================================================
// Middleware
// ============================================================
app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ============================================================
// PostgreSQL
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
// DB Init
// ============================================================
async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS staff_plan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT NOT NULL,
      employee_code TEXT,
      employee_name TEXT NOT NULL,
      employee_level TEXT,
      requester TEXT,
      po_number TEXT NOT NULL,
      work_date DATE NOT NULL,
      planned_hours NUMERIC(6,2) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

// ============================================================
// Healthcheck
// ============================================================
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch {
    res.status(500).json({ ok: false });
  }
});

// ============================================================
// STEP A – STAFFPLAN IMPORT
// ============================================================
app.post("/api/import/staffplan", async (req, res) => {
  try {
    if (!req.body.fileBase64) {
      return res.status(400).json({ ok: false, error: "fileBase64 fehlt" });
    }

    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const workbook = XLSX.read(buffer, { type: "buffer" });

    const sheet = workbook.Sheets["Staffplan"];
    if (!sheet) {
      return res.status(400).json({ ok: false, error: "Tabellenblatt 'Staffplan' fehlt" });
    }

    const calendarWeek = sheet["L2"]?.v;
    if (!calendarWeek) {
      return res.status(400).json({ ok: false, error: "Kalenderwoche (L2) fehlt" });
    }

    // Datumszeile: L4 →
    const dates = [];
    for (let c = 11; c < 200; c++) {
      const cell = sheet[XLSX.utils.encode_cell({ r: 3, c })];
      if (!cell) break;

      const d = XLSX.SSF.parse_date_code(cell.v);
      if (!d) break;

      dates.push({
        col: c,
        date: new Date(d.y, d.m - 1, d.d)
      });
    }

    let imported = 0;

    // Mitarbeiter ab Zeile 6, immer +2
    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })];
      if (!nameCell) break;

      const employee_name = String(nameCell.v || "").trim();
      if (!employee_name) continue;

      const employee_code = sheet[XLSX.utils.encode_cell({ r, c: 3 })]?.v || "";
      const po_number = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v || "";
      const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || "";
      const employee_level = sheet[XLSX.utils.encode_cell({ r, c: 7 })]?.v || "";

      if (!po_number) continue;

      for (const d of dates) {
        const hoursCell = sheet[XLSX.utils.encode_cell({ r, c: d.col })];
        if (!hoursCell || isNaN(hoursCell.v)) continue;

        await pool.query(
          `INSERT INTO staff_plan
           (calendar_week, employee_code, employee_name, employee_level,
            requester, po_number, work_date, planned_hours)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            calendarWeek,
            employee_code,
            employee_name,
            employee_level,
            requester,
            po_number,
            d.date,
            Number(hoursCell.v)
          ]
        );

        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, imported });

  } catch (err) {
    console.error("IMPORT ERROR:", err);
    res.status(500).json({ ok: false, error: "Import fehlgeschlagen" });
  }
});

// ============================================================
// DEBUG – ZEIGT WAS IN DER DB IST
// ============================================================
app.get("/api/debug/staffplan", async (req, res) => {
  const result = await pool.query(`
    SELECT DISTINCT employee_name, calendar_week, po_number
    FROM staff_plan
    ORDER BY employee_name, calendar_week, po_number
  `);
  res.json(result.rows);
});

// ============================================================
// STEP B – STUNDENNACHWEIS PDF
// ============================================================
app.get("/api/timesheet/:employee/:kw/:po", async (req, res) => {
  try {
    const { employee, kw, po } = req.params;

    const result = await pool.query(
      `SELECT * FROM staff_plan
       WHERE employee_name = $1
         AND calendar_week = $2
         AND po_number = $3
       ORDER BY work_date`,
      [employee, kw, po]
    );

    if (!result.rows.length) {
      return res.status(404).send("Keine Daten gefunden");
    }

    const rows = result.rows;

    const doc = new PDFDocument({ margin: 40 });
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `inline; filename=Stundennachweis_${employee}_${kw}_${po}.pdf`
    );
    doc.pipe(res);

    doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
    doc.moveDown();

    doc.fontSize(10);
    doc.text(`Name: ${rows[0].employee_name}`);
    doc.text(`Code / Level: ${rows[0].employee_code} / ${rows[0].employee_level}`);
    doc.text(`Ansprechpartner: ${rows[0].requester}`);
    doc.text(`PO: ${rows[0].po_number}`);
    doc.text(`Kalenderwoche: ${rows[0].calendar_week}`);
    doc.moveDown(1.5);

    doc.font("Helvetica-Bold");
    doc.text("Datum", 40);
    doc.text("Soll", 150);
    doc.text("Ist", 220);
    doc.text("Tätigkeit", 290);
    doc.font("Helvetica");
    doc.moveDown();

    let sum = 0;

    rows.forEach(r => {
      const date = new Date(r.work_date).toLocaleDateString("de-DE");
      const h = Number(r.planned_hours);
      sum += h;

      doc.text(date, 40);
      doc.text(h.toFixed(2), 150);
      doc.text(h.toFixed(2), 220);
      doc.text("Montage", 290);
      doc.moveDown();
    });

    doc.moveDown();
    doc.font("Helvetica-Bold");
    doc.text(`Summe: ${sum.toFixed(2)} Std`);
    doc.font("Helvetica");

    doc.moveDown(2);
    doc.text("Datum: ____________________________");
    doc.moveDown();
    doc.text("Unterschrift Kunde: ____________________________");
    doc.moveDown();
    doc.text(`Name in Druckbuchstaben: ${rows[0].employee_name.toUpperCase()}`);

    doc.end();

  } catch (err) {
    console.error("PDF ERROR:", err);
    res.status(500).send("PDF-Erstellung fehlgeschlagen");
  }
});

// ============================================================
// Admin Page
// ============================================================
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// ============================================================
// Start Server
// ============================================================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
