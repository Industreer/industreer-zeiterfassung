// backend/lib/staffplanProjectMapping.js
// Lädt "latest staffplan wins" als schnelles Lookup: key = `${employee_id}|${YYYY-MM-DD}`
//
// Erwartete staffplan-Spalten (mindestens):
// - employee_id (string)
// - work_date (date/timestamp)
// - project_short (string)
// - customer_po (string/null)
// - internal_po (string/null)
// - customer (string/null)
// - updated_at (timestamp) ODER created_at
//
// Falls ihr kein updated_at habt: unten auf created_at umstellen.

function toYMD(d) {
  // d kann Date, string oder timestamp sein
  const dt = (d instanceof Date) ? d : new Date(d);
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const day = String(dt.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

async function loadStaffplanMapping(db, { from, to }) {
  // from/to im Format YYYY-MM-DD
  const sql = `
    SELECT DISTINCT ON (sp.employee_id, sp.work_date::date)
      sp.employee_id,
      sp.work_date::date AS work_date,
      sp.project_short,
      sp.customer_po,
      sp.internal_po,
      sp.customer
    FROM staffplan sp
    WHERE sp.work_date::date BETWEEN $1::date AND $2::date
ORDER BY
  sp.employee_id,
  sp.work_date::date,
  sp.id DESC
  `;

  const { rows } = await db.query(sql, [from, to]);

  const map = new Map();
  for (const r of rows) {
    const key = `${r.employee_id}|${toYMD(r.work_date)}`;
    map.set(key, {
      project_id: r.project_short || null,
      project_short: r.project_short || null,
      customer_po: r.customer_po || null,
      internal_po: r.internal_po || null,
      customer: r.customer || null,
      _source: "staffplan",
    });
  }
  return map;
}

module.exports = { loadStaffplanMapping, toYMD };

