// backend/import.js
const { parse } = require("csv-parse/sync");

function parseCsv(buffer) {
  return parse(buffer, {
    columns: true,
    skip_empty_lines: true,
    trim: true
  });
}

module.exports = { parseCsv };
