// ============================================================
// INDUSTREER ZEITERFASSUNG – BACKEND (FINAL PDF FIX)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

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

// ================= HEALTH =================
app.get("/api/health", async (req, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

// ================= IMPORT =================
app.post("/api/import/staffplan", async (req, res) => {
  try {
    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];

    const calendarWeek = sheet["L2"]?.v;
    const dates = [];

    for (let c = 11; c < 200; c++) {
      const cell = sheet[XLSX.utils.encode_cell({ r: 3, c })];
      if (!cell) break;
      const d = XLSX.SSF.parse_date_code(cell.v);
      if (!d) break;
      dates.push({ col: c, date: new Date(d.y, d.m - 1, d.d) });
    }

    let imported = 0;

    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })];
      if (!nameCell) break;

      const employee_name = String(nameCell.v || "").trim();
      if (!employee_name) continue;

      const employee_code = sheet[XLSX.utils.encode_cell({ r, c: 3 })]?.v || "";
      const po_number = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v || "";
      const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || "";
      const employee_level = sheet[XLSX.utils.encode_cell({ r, c: 7 })]?.v || "";

      for (const d of dates) {
        const cell = sheet[XLSX.utils.encode_cell({ r, c: d.col })];
        if (!cell) continue;

        let hours = null;
        if (typeof cell.v === "number") hours = cell.v;
        else if (typeof cell.w === "string") {
          const m = cell.w.replace(",", ".").match(/[\d.]+/);
          if (m) hours = parseFloat(m[0]);
        }

        if (!hours || isNaN(hours) || hours <= 0) continue;

        await pool.query(
          `INSERT INTO staff_plan
           (calendar_week, employee_code, employee_name, employee_level,
            requester, po_number, work_date, planned_hours)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [calendarWeek, employee_code, employee_name, employee_level,
           requester, po_number, d.date, hours]
        );

        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, imported });

  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false });
  }
});

// ================= DEBUG =================
app.get("/api/debug/staffplan", async (req, res) => {
  const r = await pool.query(`
    SELECT employee_name, calendar_week, po_number,
           work_date, SUM(planned_hours) AS hours
    FROM staff_plan
    GROUP BY employee_name, calendar_week, po_number, work_date
    ORDER BY employee_name, work_date
  `);
  res.json(r.rows);
});

// ================= PDF (FIXED) =================
app.get("/api/timesheet/:employee/:kw/:po", async (req, res) => {
  const { employee, kw, po } = req.params;

  const r = await pool.query(
    `SELECT work_date, SUM(planned_hours) AS hours
     FROM staff_plan
     WHERE employee_name=$1 AND calendar_week=$2 AND po_number=$3
     GROUP BY work_date
     ORDER BY work_date`,
    [employee, kw, po]
  );

  if (!r.rows.length) return res.status(404).send("Keine Daten");

  const doc = new PDFDocument({ margin: 40 });
  res.setHeader("Content-Type", "application/pdf");
  doc.pipe(res);

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();

  let sum = 0;

  r.rows.forEach(row => {
    const h = Number(row.hours);
    sum += h;
    doc.text(`${new Date(row.work_date).toLocaleDateString("de-DE")}  ${h.toFixed(2)} Std`);
  });

  doc.moveDown();
  doc.text(`Summe: ${sum.toFixed(2)} Std`);
  doc.end();
});

initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
