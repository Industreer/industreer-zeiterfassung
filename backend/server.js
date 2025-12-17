// backend/server.js
const express = require("express");
const app = express();

const PORT = process.env.PORT || 3000;

app.get("/api/health", (req, res) => {
  res.json({ ok: true, message: "INDUSTREER Backend läuft" });
});

app.get("/admin", (req, res) => {
  res.send("<h1>Adminbereich läuft 🎉</h1><p>Backend erfolgreich gestartet.</p>");
});

app.listen(PORT, () => {
  console.log("Server läuft auf Port " + PORT);
});
