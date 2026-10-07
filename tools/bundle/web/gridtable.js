// gridtable.js: shared virtualized results table of the Duct web islands
// (ADR 0012). Plain DOM, no dependencies: fed with compact JSON (columns +
// rows arrays), it renders only the rows in view — a 1,000-row result is a
// few dozen <div>s, not 10,000 <td>s — with a sticky header, type badges and
// the copy affordances of the design system.
//
// Consumers:
//   - tools/bundle/web/grid.js  -> <duct-grid data-spec="..."> (light DOM)
//   - tools/bundle/web/charts.js -> dashboard table charts (shadow DOM)
// The CSS lives here (injected once per root) because shadow roots do not
// inherit the page stylesheet; colours are the shared --duct-* tokens.

export const GRID_CSS = `
duct-grid{display:block;min-height:0}
#results{height:100%;min-height:0;display:flex;flex-direction:column}
#results>[data-results]{flex:1 1 auto;min-height:0;display:flex;flex-direction:column}
#results>[data-results]>duct-grid{flex:1 1 auto;min-height:0}
.dgrid{display:flex;flex-direction:column;overflow:auto;position:relative;border:1px solid var(--duct-border);border-radius:.5rem;background:var(--duct-surface);max-height:100%}
.dgrid:focus-visible{outline:2px solid var(--duct-orange);outline-offset:-2px}
.dgrid-inner{position:relative;min-width:100%;width:max-content}
.dgrid-head{position:sticky;top:0;z-index:2;display:grid;background:var(--duct-surface-2);color:var(--duct-muted);border-bottom:1px solid var(--duct-border)}
.dgrid-hcell{display:flex;align-items:center;gap:.375rem;padding:0 .625rem;height:1.75rem;min-width:0;font-size:.6875rem;font-weight:600;letter-spacing:.02em;text-transform:uppercase;white-space:nowrap;overflow:hidden}
.dgrid-hcell.dgrid-idx{justify-content:flex-end;padding-right:.5rem}
.dgrid-name{overflow:hidden;text-overflow:ellipsis}
.dgrid-badge{display:inline-flex;align-items:center;border-radius:999px;padding:0 .375rem;height:.9375rem;font-size:.5625rem;font-weight:600;letter-spacing:.03em;text-transform:uppercase;flex:none;border:1px solid transparent}
.dgrid-badge-num{background:var(--duct-info-bg);color:var(--duct-info);border-color:color-mix(in srgb,var(--duct-info) 25%,transparent)}
.dgrid-badge-str{background:var(--duct-success-bg);color:var(--duct-success);border-color:color-mix(in srgb,var(--duct-success) 25%,transparent)}
.dgrid-badge-bool{background:var(--duct-accent-bg);color:var(--duct-orange-strong);border-color:color-mix(in srgb,var(--duct-orange) 30%,transparent)}
.dgrid-badge-time{background:#f3ebfd;color:#7a4bd6;border-color:#7a4bd633}
.dgrid-badge-muted{background:var(--duct-surface-2);color:var(--duct-muted);border-color:var(--duct-border)}
.dgrid-body{position:relative}
.dgrid-row{position:absolute;left:0;top:0;display:grid;height:26px;align-items:center;border-bottom:1px solid color-mix(in srgb,var(--duct-border) 55%,transparent)}
.dgrid-row:hover .dgrid-cell{background:var(--duct-hover)}
.dgrid-idx{display:flex;align-items:center;justify-content:flex-end;gap:.25rem;padding:0 .375rem 0 .5rem;font-family:var(--font-mono);font-size:.6875rem;color:var(--duct-muted);white-space:nowrap}
.dgrid-cell{display:flex;align-items:center;padding:0 .625rem;min-width:0;font-family:var(--font-mono);font-size:.75rem;line-height:1;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;cursor:copy;user-select:text}
.dgrid-cell.dgrid-num{justify-content:flex-end}
.dgrid-cell[data-copied="true"]{background:var(--duct-accent-bg);outline:1px solid var(--duct-orange);outline-offset:-1px}
.dgrid-cell.dgrid-null{font-style:italic;color:var(--duct-muted);cursor:default}
.dgrid-rowcopy{opacity:0;border:none;background:transparent;color:var(--duct-muted);cursor:pointer;padding:.125rem;border-radius:.25rem;line-height:0;flex:none}
.dgrid-row:hover .dgrid-rowcopy,.dgrid-rowcopy:focus-visible{opacity:1}
.dgrid-rowcopy:hover{color:var(--duct-text);background:var(--duct-active)}
.dgrid-empty{padding:1.5rem 1rem;text-align:center;color:var(--duct-muted);font-size:.8125rem}
.dgrid.dgrid-bare{border:none;border-radius:0;background:transparent;height:100%}
`;

const ROW_H = 26; // row height in px (kept in sync with GRID_CSS)
const COPY_ICON =
  '<svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="1.4" aria-hidden="true"><rect x="5.5" y="5.5" width="8" height="8" rx="1.5"/><path d="M10.5 5.5v-2a1.5 1.5 0 0 0-1.5-1.5H4A1.5 1.5 0 0 0 2.5 3.5V9A1.5 1.5 0 0 0 4 10.5h1.5"/></svg>';

// typeKind mirrors typeKind in modules/web/grid.templ (badge colour).
export function typeKind(t) {
  const u = String(t || "").toUpperCase();
  if (/(INT|DEC|NUM|FLOAT|DOUBLE|REAL|MONEY)/.test(u)) return "num";
  if (/(CHAR|TEXT|STRING|BLOB|UUID|JSON|ENUM|LIST|STRUCT|MAP|UNION)/.test(u)) return "str";
  if (/BOOL/.test(u)) return "bool";
  if (/(DATE|TIME|INTERVAL|YEAR)/.test(u)) return "time";
  return "other";
}

function badgeClass(kind) {
  return kind === "num" ? "dgrid-badge-num" : kind === "str" ? "dgrid-badge-str" : kind === "bool" ? "dgrid-badge-bool" : kind === "time" ? "dgrid-badge-time" : "dgrid-badge-muted";
}

function shortType(t) {
  t = String(t);
  return t.length > 18 ? t.slice(0, 18) + "…" : t;
}

// ensureStyles injects GRID_CSS once per root (document head or shadow root).
function ensureStyles(host) {
  const root = host.getRootNode();
  const target = root && root.head ? root.head : root === host ? host : root;
  if (target.querySelector && target.querySelector("style[data-duct-grid]")) return;
  const style = host.ownerDocument.createElement("style");
  style.setAttribute("data-duct-grid", "");
  style.textContent = GRID_CSS;
  target.appendChild(style);
}

function el(doc, tag, cls) {
  const n = doc.createElement(tag);
  if (cls) n.className = cls;
  return n;
}

// colWidths estimates a width per column from the header and a sample of rows.
function colWidths(columns, rows) {
  const widths = [56]; // the row-number gutter
  const sample = Math.min(rows.length, 400);
  for (let c = 0; c < columns.length; c++) {
    let max = String(columns[c].name || "").length + 4;
    const t = columns[c].type || columns[c].kind;
    if (t) max = Math.max(max, String(t).length + 4);
    for (let r = 0; r < sample; r++) {
      const v = rows[r][c];
      if (v != null) max = Math.max(max, String(v).length);
    }
    widths.push(Math.max(72, Math.min(420, Math.round(max * 7.4 + 26))));
  }
  return widths;
}

// mountGrid renders spec into host (replacing its content) with row
// virtualization. opts: label, format(value, colIndex, column),
// right(colIndex, column). Returns a controller { exportCSV, destroy }.
export function mountGrid(host, spec, opts) {
  opts = opts || {};
  const t0 = performance.now();
  const doc = host.ownerDocument;
  const columns = (spec && spec.columns) || [];
  const rows = (spec && spec.rows) || [];
  ensureStyles(host);
  host.textContent = "";

  if (!columns.length || !rows.length) {
    const empty = el(doc, "div", "dgrid-empty");
    empty.textContent = rows.length === 0 && columns.length ? "La consulta no devolvió filas." : "La consulta no devolvió columnas.";
    host.appendChild(empty);
    return { exportCSV() {}, destroy() {} };
  }

  const widths = colWidths(columns, rows);
  const template = widths.map((w) => w + "px").join(" ");
  const total = widths.reduce((a, b) => a + b, 0);

  const box = el(doc, "div", "dgrid" + (opts.bare ? " dgrid-bare" : ""));
  box.tabIndex = 0;
  box.setAttribute("role", "region");
  box.setAttribute("aria-label", opts.label || "Resultados de la consulta");

  const inner = el(doc, "div", "dgrid-inner");
  inner.style.width = total + "px";

  // header (sticky)
  const head = el(doc, "div", "dgrid-head");
  head.style.gridTemplateColumns = template;
  head.setAttribute("role", "row");
  const idx = el(doc, "span", "dgrid-hcell dgrid-idx");
  idx.textContent = "#";
  head.appendChild(idx);
  columns.forEach((c, i) => {
    const h = el(doc, "span", "dgrid-hcell");
    h.style.width = widths[i + 1] + "px";
    const name = el(doc, "span", "dgrid-name");
    name.textContent = c.name;
    name.title = c.name;
    h.appendChild(name);
    if (c.type) {
      const b = el(doc, "span", "dgrid-badge " + badgeClass(typeKind(c.type)));
      b.textContent = shortType(c.type);
      b.title = c.type;
      h.appendChild(b);
    }
    head.appendChild(h);
  });
  inner.appendChild(head);

  // body: absolutely positioned rows, only the window in the DOM
  const body = el(doc, "div", "dgrid-body");
  body.style.height = rows.length * ROW_H + "px";

  let first = -1;
  let last = -1;
  let frame = 0;

  function cell(i, c) {
    const col = columns[c];
    const v = rows[i][c];
    const s = el(doc, "span", "dgrid-cell");
    s.setAttribute("data-col", col.name);
    s.setAttribute("data-copy", "");
    const isNull = v == null;
    const text = isNull ? "" : opts.format ? opts.format(v, c, col) : String(v);
    s.textContent = isNull ? "NULL" : text;
    if (isNull) {
      s.classList.add("dgrid-null");
      s.title = "NULL";
    } else {
      s.title = text;
      if (opts.right ? opts.right(c, col) : col.kind === "measure") s.classList.add("dgrid-num");
    }
    return s;
  }

  function rowEl(i) {
    const tr = el(doc, "div", "dgrid-row");
    tr.style.transform = "translateY(" + i * ROW_H + "px)";
    tr.style.gridTemplateColumns = template;
    tr.setAttribute("role", "row");
    const n = el(doc, "span", "dgrid-idx");
    n.textContent = String(i + 1);
    const btn = el(doc, "button", "row-copy dgrid-rowcopy");
    btn.type = "button";
    btn.setAttribute("data-copy-row", "");
    btn.title = "Copiar fila (TSV)";
    btn.setAttribute("aria-label", "Copiar fila " + (i + 1));
    btn.innerHTML = COPY_ICON;
    n.appendChild(btn);
    tr.appendChild(n);
    for (let c = 0; c < columns.length; c++) tr.appendChild(cell(i, c));
    return tr;
  }

  function renderWindow() {
    const st = box.scrollTop;
    const vh = box.clientHeight || 400;
    const f = Math.max(0, Math.floor(st / ROW_H) - 6);
    const l = Math.min(rows.length, Math.ceil((st + vh) / ROW_H) + 6);
    if (f === first && l === last) return;
    first = f;
    last = l;
    const frag = doc.createDocumentFragment();
    for (let i = f; i < l; i++) frag.appendChild(rowEl(i));
    body.textContent = "";
    body.appendChild(frag);
  }

  box.addEventListener("scroll", function () {
    if (frame) return;
    frame = requestAnimationFrame(function () {
      frame = 0;
      renderWindow();
    });
  });
  if (typeof ResizeObserver !== "undefined") {
    const ro = new ResizeObserver(function () {
      renderWindow();
    });
    ro.observe(box);
    box.__ro = ro;
  }

  inner.appendChild(body);
  box.appendChild(inner);
  host.appendChild(box);
  renderWindow();

  const builtAt = performance.now();
  // Timestamps for the performance harness (Performance API + console).
  performance.mark("duct-grid:render");
  requestAnimationFrame(function () {
    requestAnimationFrame(function () {
      const paintAt = performance.now();
      performance.mark("duct-grid:paint");
      host.__timing = {
        rows: rows.length,
        cols: columns.length,
        buildMs: Math.round((builtAt - t0) * 10) / 10,
        paintMs: Math.round((paintAt - t0) * 10) / 10,
        domNodes: host.querySelectorAll("*").length,
      };
      console.log("[duct-grid] " + JSON.stringify(host.__timing));
    });
  });

  return {
    exportCSV() {
      const name = "duct-resultados.csv";
      const lines = [columns.map((c) => csvCell(c.name)).join(",")];
      for (let i = 0; i < rows.length; i++) {
        const vals = new Array(columns.length);
        for (let c = 0; c < columns.length; c++) {
          const v = rows[i][c];
          vals[c] = csvCell(v == null ? "" : opts.format ? opts.format(v, c, columns[c]) : String(v));
        }
        lines.push(vals.join(","));
      }
      const blob = new Blob(["\ufeff" + lines.join("\r\n") + "\r\n"], { type: "text/csv;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const a = doc.createElement("a");
      a.href = url;
      a.download = name;
      doc.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(function () {
        URL.revokeObjectURL(url);
      }, 4000);
    },
    destroy() {
      if (box.__ro) box.__ro.disconnect();
    },
  };
}

export function csvCell(v) {
  v = String(v == null ? "" : v);
  return /[",\r\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
}
