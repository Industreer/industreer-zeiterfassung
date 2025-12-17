// backend/server.js
const express = require("express");
const path = require("path");
const { pool, initDb } = require("./db");
const { parseCsv } = require("./import.js");

const app = express();
const PORT = process.env.PORT || 10000;

// JSON (auch für CSV-Text)
app.use(express.json({ limit: "5mb" }));

// Frontend ausliefern
app.use(express.static(path.join(__dirname, "..", "frontend")));

// --------------------
// Admin-Seite
// --------------------
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// --------------------
// Healthcheck (DB)
// --------------------
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, message: "DB verbunden" });
  } catch (e) {
    res.status(500).json({ ok: false, error: "DB nicht erreichbar" });
  }
});

// --------------------
// Mitarbeiter API
// --------------------
app.post("/api/employees", async (req, res) => {
  const { employee_id, name, language } = req.body;
  if (!employee_id || !name) {
    return res.status(400).json({ error: "employee_id und name erforderlich" });
  }

  await pool.query(
    `INSERT INTO employees (employee_id, name, language)
     VALUES ($1,$2,$3)
     ON CONFLICT (employee_id)
     DO UPDATE SET name=$2, language=$3`,
    [employee_id, name, language || "de"]
  );

  res.json({ ok: true });
});

app.get("/api/employees", async (req, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, language FROM employees ORDER BY employee_id"
  );
  res.json({ employees: r.rows });
});

// --------------------
// Projekte API
// --------------------
app.post("/api/projects", async (req, res) => {
  const { project_id, name } = req.body;
  if (!project_id || !name) {
    return res.status(400).json({ error: "project_id und name erforderlich" });
  }

  await pool.query(
    `INSERT INTO projects (project_id, name)
     VALUES ($1,$2)
     ON CONFLICT (project_id)
     DO UPDATE SET name=$2`,
    [project_id, name]
  );

  res.json({ ok: true });
});

app.get("/api/projects", async (req, res) => {
  const r = await pool.query(
    "SELECT project_id, name FROM projects ORDER BY project_id"
  );
  res.json({ projects: r.rows });
});

// --------------------
// CSV-IMPORT: Mitarbeiter
// --------------------
app.post("/api/import/employees", async (req, res) => {
  if (!req.body.csv) {
    return res.status(400).json({ error: "CSV fehlt" });
  }

  const rows = parseCsv(req.body.csv);
  let imported = 0;

  for (const r of rows) {
    if (!r.employee_id || !r.name) continue;

    await pool.query(
      `INSERT INTO employees (employee_id, name, language)
       VALUES ($1,$2,$3)
       ON CONFLICT (employee_id)
       DO UPDATE SET name=$2, language=$3`,
      [r.employee_id, r.name, r.language || "de"]
    );
    imported++;
  }

  res.json({ ok: true, imported });
});

// --------------------
// CSV-IMPORT: Projekte
// --------------------
app.post("/api/import/projects", async (req, res) => {
  if (!req.body.csv) {
    return res.status(400).json({ error: "CSV fehlt" });
  }

  const rows = parseCsv(req.body.csv);
  let imported = 0;

  for (const r of rows) {
    if (!r.project_id || !r.name) continue;

    await pool.query(
      `INSERT INTO projects (project_id, name)
       VALUES ($1,$2)
       ON CONFLICT (project_id)
       DO UPDATE SET name=$2`,
      [r.project_id, r.name]
    );
    imported++;
  }

  res.json({ ok: true, imported });
});

// --------------------
// Start
// --------------------
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port " + PORT);
  });
});
