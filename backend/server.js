// ============================================================
// INDUSTREER ZEITERFASSUNG – BACKEND (FINAL IMPORT ROW+1 + PDF)
// - Healthcheck
// - Staffplan Excel Import (Name-Zeile, Stunden-Zeile darunter)
// - Clear Endpoint (um falsche Importe zu löschen)
// - Debug Route
// - PDF Timesheet (pro Tag summiert)
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

// ----------------- Helpers -----------------
function parseHoursFromCell(cell) {
  if (!cell) return null;

  // 1) Numerisch (direkt)
  if (typeof cell.v === "number" && Number.isFinite(cell.v)) {
    return cell.v;
  }

  // 2) Anzeige-String (z. B. "8", "8,0", "8 Std")
  if (typeof cell.w === "string" && cell.w.trim()) {
    const cleaned = cell.w
      .replace(",", ".")
      .match(/[\d.]+/);

    if (cleaned && cleaned[0]) {
      const n = parseFloat(cleaned[0]);
      if (Number.isFinite(n)) return n;
    }
  }

  // 3) Fallback auf String v
  if (typeof cell.v === "string" && cell.v.trim()) {
    const cleaned = cell.v
      .replace(",", ".")
      .match(/[\d.]+/);

    if (cleaned && cleaned[0]) {
      const n = parseFloat(cleaned[0]);
      if (Number.isFinite(n)) return n;
    }
  }

  return null;
}

function parseDateFromHeaderCell(cell) {
  // In der Datumskopfzeile ist es oft eine Excel-Serienzahl
  if (!cell) return null;
  if (typeof cell.v === "number") {
    const d = XLSX.SSF.parse_date_code(cell.v);
    if (!d) return null;
    return new Date(d.y, d.m - 1, d.d);
  }
  if (cell.v instanceof Date) {
    return new Date(cell.v.getFullYear(), cell.v.getMonth(), cell.v.getDate());
  }
  // String-Notfälle (selten)
  if (typeof cell.v === "string") {
    const s = cell.v.trim();
    // dd.mm.yyyy
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(s)) {
      const [dd, mm, yyyy] = s.split(".");
      return new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    }
  }
  return null;
}

// ----------------- Routes -----------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch {
    res.status(500).json({ ok: false });
  }
});

// ✅ Clear staff_plan (einmalig nutzen, um falschen Import zu löschen)
app.post("/api/staffplan/clear", async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE staff_plan RESTART IDENTITY");
    res.json({ ok: true });
  } catch (e) {
    console.error("CLEAR ERROR:", e);
    res.status(500).json({ ok: false, error: "Konnte staff_plan nicht leeren" });
  }
});

// STEP A – Import
app.post("/api/import/staffplan", async (req, res) => {
  try {
    if (!req.body.fileBase64) {
      return res.status(400).json({ ok: false, error: "fileBase64 fehlt" });
    }

    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const workbook = XLSX.read(buffer, { type: "buffer" });

    // robust: erstes Blatt verwenden
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    if (!sheet) return res.status(400).json({ ok: false, error: "Kein Tabellenblatt gefunden" });

    // KW aus L2
    const calendarWeek = sheet["L2"]?.v ? String(sheet["L2"].v).trim() : "";
    if (!calendarWeek) return res.status(400).json({ ok: false, error: "Kalenderwoche (L2) fehlt" });

    // Datumszeile: Zeile 4 ab Spalte L
    const dates = [];
    for (let c = 11; c < 200; c++) {
      const headerCell = sheet[XLSX.utils.encode_cell({ r: 3, c })];
      if (!headerCell) break;

      const dt = parseDateFromHeaderCell(headerCell);
      if (!dt) break;

      dates.push({ col: c, date: dt });
    }
    if (!dates.length) {
      return res.status(400).json({ ok: false, error: "Keine Datums-Spalten ab L4 gefunden" });
    }

    let imported = 0;
    let employeesSeen = 0;

    // Mitarbeiter: Start Zeile 6 => r=5 (0-basiert), immer +2
    // Name/PO/Requester/Level stehen in der "Name-Zeile"
    // Stunden stehen in der ZEILE DARUNTER (r+1)  ✅ Fix
    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })]; // Spalte I
      if (!nameCell) break;

      const employee_name = String(nameCell.v || "").trim();
      if (!employee_name) continue;

      employeesSeen++;

      const employee_code = sheet[XLSX.utils.encode_cell({ r, c: 3 })]?.v || ""; // D
      const po_number = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v || "";     // E
      const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || "";     // G
      const employee_level = sheet[XLSX.utils.encode_cell({ r, c: 7 })]?.v || ""; // H

      if (!po_number) continue;

      const hoursRow = r + 1; // ✅ Stundenzeile

      for (const d of dates) {
        const hoursCell = sheet[XLSX.utils.encode_cell({ r: hoursRow, c: d.col })];
        const hours = parseHoursFromCell(hoursCell);

        // Filter: nur sinnvolle Stunden (z. B. 0.5 bis 24)
        if (hours === null) continue;
        if (!Number.isFinite(hours)) continue;
        if (hours <= 0) continue;
        if (hours > 24) continue; // ✅ verhindert 339/678 etc.

        await pool.query(
          `INSERT INTO staff_plan
           (calendar_week, employee_code, employee_name, employee_level, requester, po_number, work_date, planned_hours)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
          [
            calendarWeek,
            String(employee_code).trim(),
            employee_name,
            String(employee_level).trim(),
            String(requester).trim(),
            String(po_number).trim(),
            d.date,
            Number(hours)
          ]
        );

        imported++;
      }
    }

    res.json({ ok: true, calendarWeek, employeesSeen, imported });
  } catch (e) {
    console.error("IMPORT ERROR:", e);
    res.status(500).json({ ok: false, error: "Import fehlgeschlagen" });
  }
});

// Debug
app.get("/api/debug/staffplan", async (req, res) => {
  const r = await pool.query(`
    SELECT employee_name, calendar_week, po_number, work_date, SUM(planned_hours) AS hours
    FROM staff_plan
    GROUP BY employee_name, calendar_week, po_number, work_date
    ORDER BY employee_name, work_date
    LIMIT 200
  `);
  res.json(r.rows);
});

// STEP B – PDF (pro Tag summiert)
app.get("/api/timesheet/:employee/:kw/:po", async (req, res) => {
  try {
    const { employee, kw, po } = req.params;

    const r = await pool.query(
      `SELECT work_date, SUM(planned_hours) AS hours
       FROM staff_plan
       WHERE employee_name = $1 AND calendar_week = $2 AND po_number = $3
       GROUP BY work_date
       ORDER BY work_date`,
      [employee, kw, po]
    );

    if (!r.rows.length) return res.status(404).send("Keine Daten gefunden");

    const doc = new PDFDocument({ margin: 40 });
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", "inline; filename=Stundennachweis.pdf");
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
  } catch (e) {
    console.error("PDF ERROR:", e);
    res.status(500).send("PDF-Erstellung fehlgeschlagen");
  }
});

// Admin Page
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
