// backend/server.js
const express = require("express");
const path = require("path");
const app = express();

const PORT = process.env.PORT || 10000;

// JSON erlauben
app.use(express.json());

// Statische Dateien aus /frontend ausliefern
app.use(express.static(path.join(__dirname, "..", "frontend")));

// Admin-Seite
app.get("/admin", (req, res) => {
  res.sendFile(path.join(__dirname, "..", "frontend", "admin.html"));
});

// Healthcheck
app.get("/api/health", (req, res) => {
  res.json({ ok: true, message: "INDUSTREER Backend läuft" });
});

app.listen(PORT, () => {
  console.log("Server läuft auf Port " + PORT);
});
