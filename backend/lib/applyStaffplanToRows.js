// backend/lib/applyStaffplanToRows.js
function toYMD(d) {
  const dt = (d instanceof Date) ? d : new Date(d);
  const y = dt.getUTCFullYear();
  const m = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const day = String(dt.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * rows: Array of objects with:
 * - employee_id
 * - work_date
 *
 * staffplanMap key: `${employee_id}|YYYY-MM-DD`
 */
function applyStaffplanToRows(rows, staffplanMap) {
  if (!staffplanMap || !(staffplanMap instanceof Map) || !rows?.length) return rows;

  return rows.map((r) => {
    const dateStr =
      typeof r.work_date === "string" && r.work_date.length >= 10
        ? r.work_date.slice(0, 10)
        : toYMD(r.work_date);

    const key = `${r.employee_id}|${dateStr}`;
    const sp = staffplanMap.get(key);
    if (!sp) return r;

    return {
      ...r,

      // staffplan wins
      project: sp.project_short || r.project,
      project_id: sp.project_short || r.project_id,
      project_short: sp.project_short || r.project_short,

      customer_po: sp.customer_po ?? r.customer_po,
      internal_po: sp.internal_po ?? r.internal_po,
      customer: sp.customer ?? r.customer,

      _source: "staffplan",
    };
  });
}

module.exports = { applyStaffplanToRows };
