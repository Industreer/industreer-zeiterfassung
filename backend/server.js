const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

/* =======================
   Middleware
======================= */
app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

/* =======================
   PostgreSQL
======================= */
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

/* =======================
   DB Init
======================= */
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

/* =======================
   Healthcheck
======================= */
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ ok: false });
  }
});

/* =======================
   STAFFPLAN IMPORT
======================= */
app.post("/api/import/staffplan", async (req, res) => {
  try {
    if (!req.body.fileBase64) {
      return res.status(400).json({ ok: false, error: "fileBase64 fehlt" });
    }

    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const workbook = XLSX.read(buffer, { type: "buffer" });

    const sheet = workbook.Sheets["Staffplan"];
    if (!sheet) {
      return res.status(400).json({ ok: false, error: "Sheet Staffplan fehlt" });
    }

    const calendarWeek = sheet["L2"]?.v;
    if (!calendarWeek) {
      return res.status(400).json({ ok: false, error: "L2 (KW) fehlt" });
    }

    // Datumszeile L4 →
    const dates = [];
    for (let c = 11; c < 200; c++) {
      const cell = sheet[XLSX.utils.encode_cell({ r: 3, c })];
      if (!cell) break;

      const d = XLSX.SSF.parse_date_code(cell.v);
      if (!d) break;

      dates.push({
        c,
        date: new Date(d.y, d.m - 1, d.d)
      });
    }

    let imported = 0;

    // Mitarbeiter ab Zeile 6, +2
    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })];
      if (!nameCell) break;

      const employee_name = String(nameCell.v).trim();
      if (!employee_name) continue;

      const employee_code = sheet[XLSX.utils.encode_cell({ r, c: 3 })]?.v || "";
      const po_number = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v || "";
      const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || "";
      const employee_level = sheet[XLSX.utils.encode_cell({ r, c: 7 })]?.v || "";

      if (!po_number) continue;

      for (const d of dates) {
        const hoursCell = sheet[XLSX.utils.encode_cell({ r, c: d.c })];
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
    console.error(err);
    res.status(500).json({ ok: false, error: "Import fehlgeschlagen" });
  }
});

/* =======================
   Admin Page
======================= */
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

/* =======================
   Start
======================= */
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
