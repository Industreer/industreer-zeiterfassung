// backend/server.js
const express = require("express");
const path = require("path");
const { pool, initDb } = require("./db");

const app = express();
const PORT = process.env.PORT || 10000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "..", "frontend")));

// Admin-Seite
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// Healthcheck (prüft DB-Verbindung)
app.get("/api/health", async (req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true, message: "DB verbunden" });
  } catch (err) {
    res.status(500).json({ ok: false, error: "DB nicht erreichbar" });
  }
});

// Mitarbeiter anlegen / aktualisieren (inkl. Sprache)
app.post("/api/employees", async (req, res) => {
  const { employee_id, name, language } = req.body;

  if (!employee_id || !name) {
    return res.status(400).json({ error: "employee_id und name erforderlich" });
  }

  await pool.query(
    `INSERT INTO employees (employee_id, name, language)
     VALUES ($1, $2, $3)
     ON CONFLICT (employee_id)
     DO UPDATE SET name=$2, language=$3`,
    [employee_id, name, language || "de"]
  );

  res.json({ ok: true });
});

// Mitarbeiter abrufen
app.get("/api/employees", async (req, res) => {
  const r = await pool.query(
    "SELECT employee_id, name, language FROM employees ORDER BY employee_id"
  );
  res.json({ employees: r.rows });
});

// Projekte anlegen / aktualisieren
app.post("/api/projects", async (req, res) => {
  const { project_id, name } = req.body;

  if (!project_id || !name) {
    return res.status(400).json({ error: "project_id und name erforderlich" });
  }

  await pool.query(
    `INSERT INTO projects (project_id, name)
     VALUES ($1, $2)
     ON CONFLICT (project_id)
     DO UPDATE SET name=$2`,
    [project_id, name]
  );

  res.json({ ok: true });
});

// Projekte abrufen
app.get("/api/projects", async (req, res) => {
  const r = await pool.query(
    "SELECT project_id, name FROM projects ORDER BY project_id"
  );
  res.json({ projects: r.rows });
});

// DB initialisieren und Server starten
initDb().then(() => {
  app.listen(PORT, () => {
    console.log("Server läuft auf Port " + PORT);
  });
});
