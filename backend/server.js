// ============================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS (FINAL, MIGRATION-SAFE)
// ============================================================

const express = require("express");
const path = require("path");
const XLSX = require("xlsx");
const PDFDocument = require("pdfkit");
const archiver = require("archiver");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json({ limit: "25mb" }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ================== DATABASE ==================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ================== INIT DB (SAFE MIGRATION) ==================
async function initDb() {
  // EMPLOYEES
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT NOT NULL DEFAULT 'de',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);

  // STAFF PLAN (BASIS)
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

  // 👉 MIGRATION: CUSTOMER SPALTE SICHER ERGÄNZEN
  await pool.query(`
    ALTER TABLE staff_plan
    ADD COLUMN IF NOT EXISTS customer TEXT;
  `);

  // INDIZES
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_staff_kw
    ON staff_plan (calendar_week);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_staff_kw_po
    ON staff_plan (calendar_week, po_number);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_staff_customer_req
    ON staff_plan (customer, requester);
  `);

  // EMAIL OUTBOX (später SMTP)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS email_outbox (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      email_to TEXT,
      subject TEXT NOT NULL,
      body TEXT NOT NULL,
      kw TEXT,
      status TEXT DEFAULT 'queued',
      created_at TIMESTAMP DEFAULT NOW()
    );
  `);
}

// ================== HELPERS ==================
function parseHours(cell) {
  if (!cell) return null;
  if (typeof cell.v === "number") return cell.v;
  if (typeof cell.w === "string") {
    const m = cell.w.replace(",", ".").match(/[\d.]+/);
    if (m) return parseFloat(m[0]);
  }
  return null;
}

function parseExcelDate(cell) {
  if (!cell) return null;
  if (typeof cell.v === "number") {
    const d = XLSX.SSF.parse_date_code(cell.v);
    return new Date(d.y, d.m - 1, d.d);
  }
  return null;
}

function lastCompletedKW() {
  const d = new Date();
  d.setDate(d.getDate() - 7);
  const oneJan = new Date(d.getFullYear(), 0, 1);
  const week = Math.ceil((((d - oneJan) / 86400000) + oneJan.getDay() + 1) / 7);
  return `CW${String(week).padStart(2, "0")}`;
}

function safeName(v) {
  return String(v).replace(/[\/\\:*?"<>|]/g, "_");
}

// ================== ROUTES ==================
app.get("/api/health", async (_, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

app.get("/admin", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

app.get("/employee", (_, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"));
});

// ================== STAFFPLAN CLEAR ==================
app.post("/api/staffplan/clear", async (_, res) => {
  await pool.query("TRUNCATE staff_plan RESTART IDENTITY");
  res.json({ ok: true });
});

// ================== STAFFPLAN IMPORT ==================
app.post("/api/import/staffplan", async (req, res) => {
  const buffer = Buffer.from(req.body.fileBase64, "base64");
  const wb = XLSX.read(buffer, { type: "buffer" });
  const sheet = wb.Sheets[wb.SheetNames[0]];

  const calendarWeek = sheet["L2"]?.v;
  if (!calendarWeek) return res.status(400).json({ error: "KW fehlt" });

  const dates = [];
  for (let c = 11; c < 60; c++) {
    const d = parseExcelDate(sheet[XLSX.utils.encode_cell({ r: 3, c })]);
    if (!d) break;
    dates.push({ col: c, date: d });
  }

  let imported = 0;

  for (let r = 5; r < 3000; r += 2) {
    const name = sheet[XLSX.utils.encode_cell({ r, c: 8 })]?.v;
    if (!name) break;

    const customer = sheet[XLSX.utils.encode_cell({ r, c: 0 })]?.v || "";
    const po = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v;
    const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || "";

    for (const d of dates) {
      const h = parseHours(sheet[XLSX.utils.encode_cell({ r: r + 1, c: d.col })]);
      if (!h || h <= 0 || h > 24) continue;

      await pool.query(
        `INSERT INTO staff_plan
         (calendar_week, customer, employee_name, requester, po_number, work_date, planned_hours)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [calendarWeek, customer, name, requester, po, d.date, h]
      );
      imported++;
    }
  }

  res.json({ ok: true, calendarWeek, imported });
});

// ================== ADMIN ZIP EXPORT ==================
app.get("/api/admin/pdfs", async (req, res) => {
  const { kw, po, customer, requester } = req.query;
  if (!kw) return res.status(400).json({ error: "KW fehlt" });

  const where = ["calendar_week=$1"];
  const params = [kw];
  let i = 2;

  if (po) {
    where.push(`po_number=$${i++}`);
    params.push(po);
  } else if (customer) {
    where.push(`customer=$${i++}`);
    params.push(customer);
    if (requester) {
      where.push(`requester=$${i++}`);
      params.push(requester);
    }
  }

  const r = await pool.query(
    `SELECT DISTINCT employee_name, po_number
     FROM staff_plan WHERE ${where.join(" AND ")}`,
    params
  );

  const archive = archiver("zip");
  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename=KW_${kw}.zip`);
  archive.pipe(res);

  for (const row of r.rows) {
    const data = await pool.query(
      `SELECT work_date, SUM(planned_hours) h
       FROM staff_plan
       WHERE employee_name=$1 AND po_number=$2 AND calendar_week=$3
       GROUP BY work_date ORDER BY work_date`,
      [row.employee_name, row.po_number, kw]
    );

    const doc = new PDFDocument();
    const chunks = [];
    doc.on("data", c => chunks.push(c));
    doc.on("end", () => {
      archive.append(Buffer.concat(chunks), {
        name: safeName(`${row.employee_name}_${row.po_number}.pdf`)
      });
    });

    doc.fontSize(14).text("STUNDENNACHWEIS");
    let sum = 0;
    data.rows.forEach(x => {
      sum += Number(x.h);
      doc.text(`${new Date(x.work_date).toLocaleDateString("de-DE")}  ${x.h} Std`);
    });
    doc.text(`Summe: ${sum} Std`);
    doc.end();
  }

  await archive.finalize();
});

// ================== EMPLOYEE ZIP ==================
app.get("/api/employee/pdfs/last", async (req, res) => {
  const id = req.query.employee_id;
  const kw = lastCompletedKW();

  const emp = await pool.query("SELECT name FROM employees WHERE employee_id=$1", [id]);
  if (!emp.rows.length) return res.status(404).end();

  const rows = await pool.query(
    `SELECT DISTINCT po_number FROM staff_plan
     WHERE employee_name=$1 AND calendar_week=$2`,
    [emp.rows[0].name, kw]
  );

  const archive = archiver("zip");
  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename=${kw}.zip`);
  archive.pipe(res);

  for (const r of rows.rows) {
    const pdf = new PDFDocument();
    const chunks = [];
    pdf.on("data", c => chunks.push(c));
    pdf.on("end", () => archive.append(Buffer.concat(chunks), {
      name: `${safeName(emp.rows[0].name)}_${r.po_number}.pdf`
    }));
    pdf.text(`Stundennachweis ${kw}`);
    pdf.end();
  }

  await archive.finalize();
});

// ================== START ==================
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port", PORT);
  });
});
