// ======================================================================
// INDUSTREER ZEITERFASSUNG – SERVER.JS
// RAUCHERPAUSEN WERDEN ERFASST & AUTOMATISCH ABGEZOGEN
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

// ======================================================================
// MIDDLEWARE
// ======================================================================
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(express.static(path.join(__dirname, "..", "frontend")));

// ======================================================================
// PAGES
// ======================================================================
app.get("/employee", (_, res) =>
  res.sendFile(path.join(__dirname, "..", "frontend", "employee.html"))
);

// ======================================================================
// DATABASE
// ======================================================================
const pool = new Pool({
  host: process.env.PGHOST,
  user: process.env.PGUSER,
  password: process.env.PGPASSWORD,
  database: process.env.PGDATABASE,
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

// ======================================================================
// MIGRATION
// ======================================================================
async function migrate() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS time_entries (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      work_date DATE,
      start_time TIMESTAMP,
      end_time TIMESTAMP,
      total_hours NUMERIC(6,2),
      activity TEXT
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS break_entries (
      id SERIAL PRIMARY KEY,
      employee_id TEXT,
      work_date DATE,
      start_time TIMESTAMP,
      end_time TIMESTAMP
    );
  `);
}

// ======================================================================
// ZEITBUCHUNG
// ======================================================================
app.post("/api/time/start", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  const r = await pool.query(
    `INSERT INTO time_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())
     RETURNING start_time`,
    [req.body.employee_id, today]
  );
  res.json({ ok: true, start_time: r.rows[0].start_time });
});

// ======================================================================
// RAUCHERPAUSE START
// ======================================================================
app.post("/api/break/start", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);
  await pool.query(
    `INSERT INTO break_entries (employee_id, work_date, start_time)
     VALUES ($1,$2,NOW())`,
    [req.body.employee_id, today]
  );
  res.json({ ok: true });
});

// ======================================================================
// RAUCHERPAUSE ENDE
// ======================================================================
app.post("/api/break/end", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM break_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NULL
     ORDER BY start_time DESC LIMIT 1`,
    [req.body.employee_id, today]
  );

  if (!r.rows.length) {
    return res.status(400).json({ ok: false, error: "Keine aktive Pause" });
  }

  await pool.query(
    `UPDATE break_entries SET end_time=NOW() WHERE id=$1`,
    [r.rows[0].id]
  );

  res.json({ ok: true });
});

// ======================================================================
// ARBEITSENDE (PAUSEN WERDEN ABGEZOGEN)
// ======================================================================
app.post("/api/time/end", async (req, res) => {
  const today = new Date().toISOString().slice(0, 10);

  const r = await pool.query(
    `SELECT * FROM time_entries
     WHERE employee_id=$1 AND work_date=$2
     ORDER BY start_time DESC LIMIT 1`,
    [req.body.employee_id, today]
  );

  if (!r.rows.length) return res.status(400).json({ ok: false });

  const start = new Date(r.rows[0].start_time);
  const end = new Date();

  // Summe Raucherpausen
  const breaks = await pool.query(
    `SELECT start_time, end_time FROM break_entries
     WHERE employee_id=$1 AND work_date=$2 AND end_time IS NOT NULL`,
    [req.body.employee_id, today]
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
    [netHours, req.body.activity, r.rows[0].id]
  );

  res.json({
    ok: true,
    net_hours: netHours.toFixed(2),
    break_minutes: Math.round(breakMs / 60000)
  });
});

// ======================================================================
// START
// ======================================================================
migrate().then(() => {
  app.listen(PORT, () =>
    console.log("🚀 Server läuft auf Port", PORT)
  );
});
