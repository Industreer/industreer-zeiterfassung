// ============================================================
// INDUSTREER ZEITERFASSUNG – BACKEND (ADMIN + EMPLOYEE + PDF ZIP)
// Features:
// - Staffplan Import (Excel Matrix), Customer aus Spalte A
// - Employees DB (id, name, email, language)
// - Admin: ZIP-PDFs für KW + (PO) ODER (Customer + Requester)
// - Employee: ZIP-PDFs für letzte KW & wählbare KW
// - "Email senden" als OUTBOX (kein SMTP konfiguriert)
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

const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ----------------- Helpers -----------------
function parseHoursFromCell(cell) {
  if (!cell) return null;

  if (typeof cell.v === "number" && Number.isFinite(cell.v)) return cell.v;

  if (typeof cell.w === "string" && cell.w.trim()) {
    const m = cell.w.replace(",", ".").match(/[\d.]+/);
    if (m && m[0]) {
      const n = parseFloat(m[0]);
      if (Number.isFinite(n)) return n;
    }
  }

  if (typeof cell.v === "string" && cell.v.trim()) {
    const m = cell.v.replace(",", ".").match(/[\d.]+/);
    if (m && m[0]) {
      const n = parseFloat(m[0]);
      if (Number.isFinite(n)) return n;
    }
  }

  return null;
}

function parseDateFromHeaderCell(cell) {
  if (!cell) return null;

  if (typeof cell.v === "number") {
    const d = XLSX.SSF.parse_date_code(cell.v);
    if (!d) return null;
    return new Date(d.y, d.m - 1, d.d);
  }

  if (cell.v instanceof Date) {
    return new Date(cell.v.getFullYear(), cell.v.getMonth(), cell.v.getDate());
  }

  if (typeof cell.v === "string") {
    const s = cell.v.trim();
    if (/^\d{2}\.\d{2}\.\d{4}$/.test(s)) {
      const [dd, mm, yyyy] = s.split(".");
      return new Date(Number(yyyy), Number(mm) - 1, Number(dd));
    }
  }

  return null;
}

// ISO week helpers
function getISOWeekYear(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNo = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return { year: d.getUTCFullYear(), week: weekNo };
}

// letzte abgeschlossene KW (nicht die laufende)
function lastCompletedCW() {
  const now = new Date();
  const sevenDaysAgo = new Date(now.getTime() - 7 * 86400000);
  const { week } = getISOWeekYear(sevenDaysAgo);
  return `CW${String(week).padStart(2, "0")}`;
}

function safeFilename(s) {
  return String(s)
    .replace(/[\/\\:*?"<>|]/g, "_")
    .replace(/\s+/g, " ")
    .trim();
}

async function initDb() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS employees (
      employee_id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT,
      language TEXT NOT NULL DEFAULT 'de',
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    );

    -- Staffplan: customer neu
    CREATE TABLE IF NOT EXISTS staff_plan (
      id SERIAL PRIMARY KEY,
      calendar_week TEXT NOT NULL,
      customer TEXT,
      employee_code TEXT,
      employee_name TEXT NOT NULL,
      employee_level TEXT,
      requester TEXT,
      po_number TEXT NOT NULL,
      work_date DATE NOT NULL,
      planned_hours NUMERIC(6,2) NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE INDEX IF NOT EXISTS idx_staff_plan_kw ON staff_plan (calendar_week);
    CREATE INDEX IF NOT EXISTS idx_staff_plan_kw_po ON staff_plan (calendar_week, po_number);
    CREATE INDEX IF NOT EXISTS idx_staff_plan_customer_req ON staff_plan (customer, requester);

    -- Email Outbox (bis SMTP später kommt)
    CREATE TABLE IF NOT EXISTS email_outbox (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      email_to TEXT,
      subject TEXT NOT NULL,
      body TEXT NOT NULL,
      kw TEXT,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      status TEXT NOT NULL DEFAULT 'queued'
    );
  `);
}

// PDF buffer generator (one employee + one PO + one KW)
async function buildTimesheetPdfBuffer({ employeeName, calendarWeek, poNumber }) {
  const r = await pool.query(
    `SELECT work_date, SUM(planned_hours) AS hours
     FROM staff_plan
     WHERE employee_name=$1 AND calendar_week=$2 AND po_number=$3
     GROUP BY work_date
     ORDER BY work_date`,
    [employeeName, calendarWeek, poNumber]
  );

  if (!r.rows.length) return null;

  const doc = new PDFDocument({ margin: 40 });
  const chunks = [];
  doc.on("data", c => chunks.push(c));

  doc.fontSize(16).text("STUNDENNACHWEIS", { align: "center" });
  doc.moveDown();

  doc.fontSize(10);
  doc.text(`Name: ${employeeName}`);
  doc.text(`KW: ${calendarWeek}`);
  doc.text(`PO: ${poNumber}`);
  doc.moveDown();

  let sum = 0;
  r.rows.forEach(row => {
    const h = Number(row.hours);
    sum += h;
    doc.text(`${new Date(row.work_date).toLocaleDateString("de-DE")}   ${h.toFixed(2)} Std`);
  });

  doc.moveDown();
  doc.text(`Summe: ${sum.toFixed(2)} Std`);
  doc.moveDown(2);
  doc.text("Datum: ____________________________");
  doc.moveDown();
  doc.text("Unterschrift Kunde: ____________________________");
  doc.end();

  await new Promise(resolve => doc.on("end", resolve));
  return Buffer.concat(chunks);
}

// ZIP builder for many PDFs
async function streamZipOfPdfs(res, items, zipName) {
  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename=${zipName}`);

  const archive = archiver("zip", { zlib: { level: 9 } });
  archive.on("error", err => {
    console.error("ZIP ERROR:", err);
    try { res.status(500).end("ZIP Fehler"); } catch {}
  });
  archive.pipe(res);

  for (const it of items) {
    const pdf = await buildTimesheetPdfBuffer(it);
    if (!pdf) continue;

    const file = safeFilename(`${it.employeeName}__${it.poNumber}__${it.calendarWeek}.pdf`);
    archive.append(pdf, { name: file });
  }

  await archive.finalize();
}

// ----------------- ROUTES -----------------
app.get("/api/health", async (req, res) => {
  await pool.query("SELECT 1");
  res.json({ ok: true });
});

app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

app.get("/employee", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"));
});

// -------- Employees (minimal) --------
// Add/Update employee
app.post("/api/employees/upsert", async (req, res) => {
  const { employee_id, name, email, language } = req.body || {};
  if (!employee_id || !name) {
    return res.status(400).json({ ok: false, error: "employee_id und name sind Pflicht" });
  }

  await pool.query(
    `INSERT INTO employees (employee_id, name, email, language)
     VALUES ($1,$2,$3,$4)
     ON CONFLICT (employee_id)
     DO UPDATE SET name=$2, email=$3, language=$4`,
    [
      String(employee_id).trim(),
      String(name).trim(),
      email ? String(email).trim() : null,
      language ? String(language).trim() : "de"
    ]
  );

  res.json({ ok: true });
});

// list employees
app.get("/api/employees", async (req, res) => {
  const r = await pool.query(`SELECT employee_id, name, email, language FROM employees ORDER BY name`);
  res.json(r.rows);
});

// -------- Staffplan controls --------
app.post("/api/staffplan/clear", async (req, res) => {
  await pool.query("TRUNCATE TABLE staff_plan RESTART IDENTITY");
  res.json({ ok: true });
});

// Debug (zeigt vorhandene Kombinationen)
app.get("/api/debug/staffplan", async (req, res) => {
  const r = await pool.query(`
    SELECT DISTINCT employee_name, calendar_week, po_number, customer, requester
    FROM staff_plan
    ORDER BY calendar_week, employee_name, po_number
    LIMIT 300
  `);
  res.json(r.rows);
});

// -------- Staffplan Import (Excel) --------
// Erwartet: Kunde in Spalte A (c=0) in der Name-Zeile
// Name/PO/Requester/Level in Name-Zeile r, Stunden in r+1
app.post("/api/import/staffplan", async (req, res) => {
  try {
    if (!req.body.fileBase64) {
      return res.status(400).json({ ok: false, error: "fileBase64 fehlt" });
    }

    const buffer = Buffer.from(req.body.fileBase64, "base64");
    const workbook = XLSX.read(buffer, { type: "buffer" });
    const sheet = workbook.Sheets[workbook.SheetNames[0]];
    if (!sheet) return res.status(400).json({ ok: false, error: "Kein Tabellenblatt gefunden" });

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

    for (let r = 5; r < 5000; r += 2) {
      const nameCell = sheet[XLSX.utils.encode_cell({ r, c: 8 })]; // I
      if (!nameCell) break;

      const employee_name = String(nameCell.v || "").trim();
      if (!employee_name) continue;

      employeesSeen++;

      const customer = sheet[XLSX.utils.encode_cell({ r, c: 0 })]?.v || ""; // A
      const employee_code = sheet[XLSX.utils.encode_cell({ r, c: 3 })]?.v || ""; // D
      const po_number = sheet[XLSX.utils.encode_cell({ r, c: 4 })]?.v || ""; // E
      const requester = sheet[XLSX.utils.encode_cell({ r, c: 6 })]?.v || ""; // G
      const employee_level = sheet[XLSX.utils.encode_cell({ r, c: 7 })]?.v || ""; // H

      if (!po_number) continue;

      const hoursRow = r + 1;

      for (const d of dates) {
        const cell = sheet[XLSX.utils.encode_cell({ r: hoursRow, c: d.col })];
        const hours = parseHoursFromCell(cell);

        // Filter: nur plausible Stunden
        if (hours === null) continue;
        if (!Number.isFinite(hours)) continue;
        if (hours <= 0) continue;
        if (hours > 24) continue;

        await pool.query(
          `INSERT INTO staff_plan
           (calendar_week, customer, employee_code, employee_name, employee_level, requester, po_number, work_date, planned_hours)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
          [
            calendarWeek,
            String(customer).trim() || null,
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

// -------- Admin: ZIP Export für KW + Filter --------
// Query:
// - kw (pflicht)
// - entweder po=...   ODER (customer=... und requester=... optional)
// - wenn customer gesetzt, kann requester optional sein
app.get("/api/admin/pdfs", async (req, res) => {
  const kw = String(req.query.kw || "").trim();
  const po = String(req.query.po || "").trim();
  const customer = String(req.query.customer || "").trim();
  const requester = String(req.query.requester || "").trim();

  if (!kw) return res.status(400).json({ ok: false, error: "kw ist Pflicht" });

  // Filter aufbauen
  const where = ["calendar_week = $1"];
  const params = [kw];
  let p = 2;

  if (po) {
    where.push(`po_number = $${p++}`);
    params.push(po);
  } else if (customer) {
    where.push(`customer = $${p++}`);
    params.push(customer);
    if (requester) {
      where.push(`requester = $${p++}`);
      params.push(requester);
    }
  }

  // distinct combinations (employee_name + po_number)
  const r = await pool.query(
    `SELECT DISTINCT employee_name, po_number
     FROM staff_plan
     WHERE ${where.join(" AND ")}
     ORDER BY employee_name, po_number`,
    params
  );

  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Keine Daten für Filter" });

  const items = r.rows.map(x => ({
    employeeName: x.employee_name,
    poNumber: x.po_number,
    calendarWeek: kw
  }));

  const zipName = safeFilename(`Stundennachweise_${kw}${po ? `_PO_${po}` : customer ? `_Kunde_${customer}` : ""}.zip`);
  return streamZipOfPdfs(res, items, zipName);
});

// -------- Employee: ZIP für letzte KW (alle POs) --------
// employee_id wird genutzt, Name kommt aus employees
app.get("/api/employee/pdfs/last", async (req, res) => {
  const employee_id = String(req.query.employee_id || "").trim();
  if (!employee_id) return res.status(400).json({ ok: false, error: "employee_id fehlt" });

  const emp = await pool.query(`SELECT name FROM employees WHERE employee_id=$1`, [employee_id]);
  if (!emp.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

  const employeeName = emp.rows[0].name;
  const kw = lastCompletedCW();

  const r = await pool.query(
    `SELECT DISTINCT po_number
     FROM staff_plan
     WHERE employee_name=$1 AND calendar_week=$2
     ORDER BY po_number`,
    [employeeName, kw]
  );

  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Keine Daten für letzte KW" });

  const items = r.rows.map(x => ({
    employeeName,
    poNumber: x.po_number,
    calendarWeek: kw
  }));

  const zipName = safeFilename(`Stundennachweise_${employeeName}_${kw}.zip`);
  return streamZipOfPdfs(res, items, zipName);
});

// -------- Employee: ZIP für gewählte KW (alle POs) --------
app.get("/api/employee/pdfs", async (req, res) => {
  const employee_id = String(req.query.employee_id || "").trim();
  const kw = String(req.query.kw || "").trim();
  if (!employee_id || !kw) return res.status(400).json({ ok: false, error: "employee_id und kw sind Pflicht" });

  const emp = await pool.query(`SELECT name FROM employees WHERE employee_id=$1`, [employee_id]);
  if (!emp.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });

  const employeeName = emp.rows[0].name;

  const r = await pool.query(
    `SELECT DISTINCT po_number
     FROM staff_plan
     WHERE employee_name=$1 AND calendar_week=$2
     ORDER BY po_number`,
    [employeeName, kw]
  );

  if (!r.rows.length) return res.status(404).json({ ok: false, error: "Keine Daten für diese KW" });

  const items = r.rows.map(x => ({
    employeeName,
    poNumber: x.po_number,
    calendarWeek: kw
  }));

  const zipName = safeFilename(`Stundennachweise_${employeeName}_${kw}.zip`);
  return streamZipOfPdfs(res, items, zipName);
});

// -------- Employee: "Email senden" (OUTBOX, kein SMTP) --------
app.post("/api/employee/email", async (req, res) => {
  const { employee_id, kw } = req.body || {};
  const eid = String(employee_id || "").trim();
  const week = String(kw || "").trim();
  if (!eid || !week) return res.status(400).json({ ok: false, error: "employee_id und kw sind Pflicht" });

  const emp = await pool.query(`SELECT name, email FROM employees WHERE employee_id=$1`, [eid]);
  if (!emp.rows.length) return res.status(404).json({ ok: false, error: "Mitarbeiter nicht gefunden" });
  if (!emp.rows[0].email) return res.status(400).json({ ok: false, error: "Keine Email für Mitarbeiter hinterlegt" });

  // Wir erstellen nur einen Outbox-Eintrag (später SMTP)
  await pool.query(
    `INSERT INTO email_outbox (employee_id, email_to, subject, body, kw)
     VALUES ($1,$2,$3,$4,$5)`,
    [
      eid,
      emp.rows[0].email,
      `Stundennachweise ${week}`,
      `Hallo ${emp.rows[0].name},\n\nhier sollten die Stundennachweise für ${week} automatisch als Anhang versendet werden.\nAktuell ist noch kein Mailserver konfiguriert, daher liegt die Email in der Outbox.\n\nViele Grüße\nINDUSTREER`,
      week
    ]
  );

  res.json({
    ok: true,
    message: "Email in Outbox gespeichert (SMTP später aktivierbar)."
  });
});

initDb().then(() => {
  app.listen(PORT, () => console.log("Server läuft auf Port", PORT));
});
