const PDFDocument = require("pdfkit");

/**
 * rows: Array of
 * { date:"YYYY-MM-DD", project:string, internal_po?:string|null, task?:string|null, minutes:number }
 */
function minutesToHHMM(mins) {
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return String(h).padStart(2, "0") + ":" + String(m).padStart(2, "0");
}

function isoWeek(dateStr) {
  // Returns { year, week } for ISO week
  const d = new Date(dateStr + "T00:00:00Z");
  // Thursday in current week decides the year.
  const day = (d.getUTCDay() + 6) % 7; // Mon=0..Sun=6
  d.setUTCDate(d.getUTCDate() - day + 3); // move to Thursday
  const firstThursday = new Date(Date.UTC(d.getUTCFullYear(), 0, 4));
  const firstDay = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstDay + 3);
  const week = 1 + Math.round((d - firstThursday) / (7 * 24 * 3600 * 1000));
  return { year: d.getUTCFullYear(), week };
}

function groupKey(row, mode) {
  if (mode === "date") return row.date;
  if (mode === "week") {
    const { year, week } = isoWeek(row.date);
    return `KW ${String(week).padStart(2, "0")}/${year}`;
  }
  if (mode === "project") {
    const po = row.internal_po ? ` • ${row.internal_po}` : "";
    return `${row.project || "—"}${po}`;
  }
  return "KW";
}

function groupRows(rows, mode) {
  const map = new Map();
  for (const r of rows) {
    const key = groupKey(r, mode);
    if (!map.has(key)) map.set(key, []);
    map.get(key).push(r);
  }
  const keys = Array.from(map.keys()).sort((a, b) => a.localeCompare(b, "de"));
  return keys.map((k) => [k, map.get(k)]);
}

function drawHeader(doc, { title, periodLabel, logoPath }) {
  const margin = 48; // pt
  doc.fontSize(18).font("Helvetica-Bold");

  if (logoPath) {
    try {
      doc.image(logoPath, margin, margin - 6, { fit: [140, 42] });
    } catch (_) {
      // ignore missing logo
    }
  }

  const titleX = margin + (logoPath ? 160 : 0);
  doc.text(title, titleX, margin);

  if (periodLabel) {
    doc.fontSize(10).font("Helvetica").fillColor("#444");
    doc.text(periodLabel, titleX, margin + 26);
    doc.fillColor("#000");
  }

  doc.moveDown(2);
  doc.y = margin + 70;
}

function drawTable(doc, { rows }) {
  const margin = 48;
  const pageWidth = doc.page.width;
  const usable = pageWidth - margin * 2;

  // Column widths (tuned for calm layout)
  const colW = {
    date: 72,
    project: 170,
    po: 70,
    task: usable - (72 + 170 + 70 + 70),
    time: 70,
  };

  const startX = margin;
  let y = doc.y;

  // Header row background
  doc.save();
  doc.rect(startX, y, usable, 20).fill("#F2F4F7");
  doc.restore();

  doc.fontSize(9).font("Helvetica-Bold");
  doc.fillColor("#111");
  doc.text("Datum", startX + 6, y + 6, { width: colW.date - 10 });
  doc.text("Projekt", startX + colW.date + 6, y + 6, { width: colW.project - 10 });
  doc.text("PO", startX + colW.date + colW.project + 6, y + 6, { width: colW.po - 10 });
  doc.text("Tätigkeit", startX + colW.date + colW.project + colW.po + 6, y + 6, { width: colW.task - 10 });
  doc.text("Zeit", startX + colW.date + colW.project + colW.po + colW.task + 6, y + 6, { width: colW.time - 10, align: "right" });

  // Lines
  doc.moveTo(startX, y + 20).lineTo(startX + usable, y + 20).strokeColor("#D0D5DD").lineWidth(1).stroke();

  y += 24;
  doc.font("Helvetica").fontSize(9).fillColor("#000");

  let total = 0;

  for (const r of rows) {
    const rowHeight = 18;

    // Page break if needed
    if (y + rowHeight + 40 > doc.page.height - 48) {
      doc.addPage();
      y = 48;
    }

    total += Number(r.minutes || 0);

    const d = new Date(r.date + "T00:00:00");
    const dd = String(d.getDate()).padStart(2, "0");
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const yyyy = d.getFullYear();
    const dateLabel = `${dd}.${mm}.${yyyy}`;

    doc.text(dateLabel, startX + 6, y, { width: colW.date - 10 });
    doc.text(r.project || "—", startX + colW.date + 6, y, { width: colW.project - 10 });
    doc.text(r.internal_po || "—", startX + colW.date + colW.project + 6, y, { width: colW.po - 10 });
    doc.text(r.task || "—", startX + colW.date + colW.project + colW.po + 6, y, { width: colW.task - 10 });
    doc.text(minutesToHHMM(Number(r.minutes || 0)), startX + colW.date + colW.project + colW.po + colW.task + 6, y, {
      width: colW.time - 10,
      align: "right",
    });

    // subtle row divider
    doc.moveTo(startX, y + rowHeight).lineTo(startX + usable, y + rowHeight).strokeColor("#E4E7EC").lineWidth(0.7).stroke();

    y += rowHeight + 2;
  }

  // Sum row
  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Summe", startX + colW.date + colW.project + colW.po + 6, y, { width: colW.task - 10, align: "right" });
  doc.text(minutesToHHMM(total), startX + colW.date + colW.project + colW.po + colW.task + 6, y, {
    width: colW.time - 10,
    align: "right",
  });

  doc.font("Helvetica").moveDown(2);
  doc.y = y + 22;
}

function buildErfassungsbogenPdf(res, rows, opts = {}) {
  const {
    title = "Erfassungsbogen",
    groupMode = "week", // "date" | "week" | "project"
    periodLabel = null,
    logoPath = null,
  } = opts;

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", 'inline; filename="erfassungsbogen.pdf"');

  const doc = new PDFDocument({
    size: "A4",
    margins: { top: 48, bottom: 42, left: 48, right: 48 },
    info: { Title: title },
  });

  doc.pipe(res);

  drawHeader(doc, { title, periodLabel, logoPath });

  const grouped = groupRows(rows, groupMode);

  for (let i = 0; i < grouped.length; i++) {
    const [groupTitle, items] = grouped[i];

    // Group title
    doc.font("Helvetica-Bold").fontSize(11).fillColor("#111");
    doc.text(groupTitle);
    doc.moveDown(0.5);

    // Table
    // sort rows inside group
    const sorted = [...items].sort((a, b) => (a.date || "").localeCompare(b.date || "") || (a.project || "").localeCompare(b.project || "", "de"));
    drawTable(doc, { rows: sorted });

    if (i !== grouped.length - 1) {
      doc.moveDown(0.5);
    }
  }

  doc.end();
}

module.exports = { buildErfassungsbogenPdf };
