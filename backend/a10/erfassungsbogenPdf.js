// backend/a10/erfassungsbogenPdf.js
const PDFDocument = require("pdfkit");
const { loadStaffplanMapping, toYMD } = require("../lib/staffplanProjectMapping");

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

function sumMinutes(rows) {
  return rows.reduce((acc, r) => acc + Number(r.minutes || 0), 0);
}

/**
 * Optional staffplan overrides
 * - No-op, wenn employee_id oder staffplanMap fehlt
 * - "latest staffplan wins" ist bereits im staffplanMap enthalten
 * - Überschreibt project/internal_po/customer_po/customer im Row
 * - Liefert optional metaOverride aus dem "letzten Datum" im Zeitraum
 */
function applyStaffplanOverrides(rows, { employee_id, staffplanMap }) {
  if (!employee_id || !staffplanMap) return { rows, metaOverride: null };

  const out = (rows || []).map((r) => {
    const key = `${employee_id}|${r.date}`;
    const sp = staffplanMap.get(key);
    if (!sp) return r;

    return {
      ...r,
      // staffplan wins:
      project: sp.project_short || r.project,
      internal_po: sp.internal_po ?? r.internal_po,
      customer_po: sp.customer_po ?? r.customer_po,
      customer: sp.customer ?? r.customer,
      _source: "staffplan",
    };
  });

  const lastDate = out
    .map((r) => r.date)
    .filter(Boolean)
    .sort((a, b) => a.localeCompare(b))
    .slice(-1)[0];

  const spLast = lastDate ? staffplanMap.get(`${employee_id}|${lastDate}`) : null;

  const metaOverride = spLast
    ? {
        customer: spLast.customer || null,
        customerPo: spLast.customer_po || null,
        internalPo: spLast.internal_po || null,
      }
    : null;

  return { rows: out, metaOverride };
}

function drawHeader(doc, { title, periodLabel, logoPath, metaLines }) {
  const margin = 48; // pt
  const pageW = doc.page.width;
  const contentW = pageW - margin * 2;

  // Logo (links)
  doc.fontSize(18).font("Helvetica-Bold");
  if (logoPath) {
    try {
      doc.image(logoPath, margin, margin - 6, { fit: [140, 42] });
    } catch (_) {
      // ignore missing logo
    }
  }

  // Titel + Zeitraum (links, mit Platz für Meta rechts)
  const titleX = margin + (logoPath ? 160 : 0);
  const titleWidth = contentW - (logoPath ? 160 : 0) - 170;

  doc.fillColor("#111").font("Helvetica-Bold").fontSize(16);
  doc.text(title, titleX, margin, {
    width: titleWidth,
    align: "center",
  });
  if (periodLabel) {
    const afterTitleY = doc.y + 4; // automatisch unter Titel

    doc
      .font("Helvetica")
      .fontSize(10)
      .fillColor("#444")
      .text(periodLabel, titleX, afterTitleY, {
        width: titleWidth,
        align: "center",
      });

    doc.fillColor("#000");
  }

  // Meta-Block rechts
  if (Array.isArray(metaLines) && metaLines.length) {
    const boxW = 170;
    const boxX = margin + contentW - boxW;
    const boxY = margin - 2;

    doc.save();
    doc.roundedRect(boxX, boxY, boxW, 54, 6).fill("#F8FAFC");
    doc.restore();

    doc.font("Helvetica").fontSize(9);
    let y = boxY + 10;
    for (const line of metaLines.slice(0, 4)) {
      doc.fillColor("#344054").text(line, boxX + 10, y, {
        width: boxW - 20,
        align: "right",
      });
      y += 11;
    }
    doc.fillColor("#000");
  }

  // Trennlinie unter Header
  const lineY = margin + 64;
  doc
    .moveTo(margin, lineY)
    .lineTo(margin + contentW, lineY)
    .strokeColor("#E4E7EC")
    .lineWidth(1)
    .stroke();

  doc.y = margin + 82;
}

function drawTable(doc, { rows, showKwColumn = false }) {
  const margin = 48;
  const pageWidth = doc.page.width;
  const usable = pageWidth - margin * 2;

  const colW = showKwColumn
    ? {
        kw: 56,
        date: 72,
        project: 160,
        po: 68,
        task: usable - (56 + 72 + 160 + 68 + 70),
        time: 70,
      }
    : {
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

  let x = startX;

  if (showKwColumn) {
    doc.text("KW", x + 6, y + 5, { width: colW.kw - 10 });
    x += colW.kw;
  }

  doc.text("Datum", x + 6, y + 5, { width: colW.date - 10 });
  x += colW.date;

  doc.text("Projekt", x + 6, y + 5, { width: colW.project - 10 });
  x += colW.project;

  doc.text("PO", x + 6, y + 5, { width: colW.po - 10 });
  x += colW.po;

  doc.text("Tätigkeit", x + 6, y + 5, { width: colW.task - 10 });
  x += colW.task;

  doc.text("Zeit", x + 6, y + 5, { width: colW.time - 10, align: "right" });

  // Lines
  doc
    .moveTo(startX, y + 20)
    .lineTo(startX + usable, y + 20)
    .strokeColor("#D0D5DD")
    .lineWidth(1)
    .stroke();

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

    let xx = startX;

    if (showKwColumn) {
      const { week } = isoWeek(r.date);
      doc.text(`KW${String(week).padStart(2, "0")}`, xx + 6, y, { width: colW.kw - 10 });
      xx += colW.kw;
    }

    doc.text(dateLabel, xx + 6, y, { width: colW.date - 10 });
    xx += colW.date;

    doc.text(r.project || "—", xx + 6, y, { width: colW.project - 10 });
    xx += colW.project;

    doc.text(r.internal_po || "—", xx + 6, y, { width: colW.po - 10 });
    xx += colW.po;

    doc.text(r.task || "—", xx + 6, y, { width: colW.task - 10 });
    xx += colW.task;

    doc.text(minutesToHHMM(Number(r.minutes || 0)), xx + 6, y, {
      width: colW.time - 10,
      align: "right",
    });

    // subtle row divider
    doc
      .moveTo(startX, y + rowHeight)
      .lineTo(startX + usable, y + rowHeight)
      .strokeColor("#E4E7EC")
      .lineWidth(0.7)
      .stroke();

    y += rowHeight + 2;
  }

  // Sum row
  y += 6;
  doc.font("Helvetica-Bold");
  doc.text("Summe", startX + usable - (colW.time + colW.task), y, {
    width: colW.task - 10,
    align: "right",
  });
  doc.text(minutesToHHMM(total), startX + usable - colW.time + 6, y, {
    width: colW.time - 12,
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
    meta = {}, // { customer, customerPo, internalPo }
    showKwColumn = false,
  } = opts;

  // Optional staffplan override (no-op wenn nichts übergeben)
  const { rows: rows2, metaOverride } = applyStaffplanOverrides(rows, {
    employee_id: opts.employee_id,
    staffplanMap: opts.staffplanMap,
  });

  // Meta optional aus staffplan überschreiben (wenn vorhanden)
  if (metaOverride) {
    meta.customer = metaOverride.customer || meta.customer;
    meta.customerPo = metaOverride.customerPo || meta.customerPo;
    meta.internalPo = metaOverride.internalPo || meta.internalPo;
  }

  const totalAll = sumMinutes(rows2);
  const metaLines = [
    meta.customer ? `Kunde: ${meta.customer}` : null,
    meta.customerPo ? `Kunden-PO: ${meta.customerPo}` : null,
    meta.internalPo ? `Internal-PO: ${meta.internalPo}` : null,
    `Gesamt: ${minutesToHHMM(totalAll)}`,
  ].filter(Boolean);

  res.setHeader("Content-Type", "application/pdf");
  res.setHeader("Content-Disposition", 'inline; filename="erfassungsbogen.pdf"');

  const doc = new PDFDocument({
    size: "A4",
    margins: { top: 48, bottom: 42, left: 48, right: 48 },
    info: { Title: title },
  });

  doc.pipe(res);

  drawHeader(doc, { title, periodLabel, logoPath, metaLines });

  const grouped = groupRows(rows2, groupMode);

  for (let i = 0; i < grouped.length; i++) {
    const [groupTitle, items] = grouped[i];

    // Group separator (ruhiger als Überschrift)
    if (i > 0) {
      doc.moveDown(0.6);
      const margin = 48;
      const usable = doc.page.width - margin * 2;
      const yLine = doc.y;
      doc
        .moveTo(margin, yLine)
        .lineTo(margin + usable, yLine)
        .strokeColor("#E4E7EC")
        .lineWidth(1)
        .stroke();
      doc.moveDown(0.6);
    }

    const sorted = [...items].sort(
      (a, b) =>
        (a.date || "").localeCompare(b.date || "") ||
        (a.project || "").localeCompare(b.project || "", "de")
    );

    drawTable(doc, { rows: sorted, showKwColumn });

    if (i !== grouped.length - 1) doc.moveDown(0.5);
  }

  doc.end();
}

module.exports = { buildErfassungsbogenPdf };
