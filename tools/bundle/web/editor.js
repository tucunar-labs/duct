// CodeMirror 6 island for the Duct SQL editor (ADR 0012).
//
// Contract with the page: a <div data-sql-editor> next to a <textarea> that
// holds the SQL. The editor mirrors its content into the textarea and fires
// an `input` event so Datastar's data-bind keeps the signal in sync.
// Mod-Enter clicks the element matching the data-run selector.
//
// The schema tree next to it is wired here: click inserts at the cursor,
// double-click previews a table, right click opens a context menu, rows are
// draggable into the editor (HTML5 drag & drop resolved with posAtCoords) and
// CodeMirror completes names from the role's catalog (GET /catalog.json —
// only what the session may see; keywords and alias/schema/table prefixes
// come from @codemirror/lang-sql).
import { basicSetup } from "codemirror";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState, StateEffect } from "@codemirror/state";
import { sql, StandardSQL } from "@codemirror/lang-sql";
import { HighlightStyle, syntaxHighlighting } from "@codemirror/language";
import { tags as t } from "@lezer/highlight";

// Token colours come from CSS variables so light/dark follow the page.
const highlight = HighlightStyle.define([
  { tag: [t.keyword, t.operatorKeyword], color: "var(--duct-orange-strong)", fontWeight: "600" },
  { tag: [t.string, t.special(t.string)], color: "var(--duct-tok-string)" },
  { tag: [t.number, t.bool, t.null], color: "var(--duct-tok-number)" },
  { tag: [t.typeName, t.standard(t.name)], color: "var(--duct-tok-type)" },
  { tag: [t.lineComment, t.blockComment], color: "var(--duct-muted)", fontStyle: "italic" },
]);

const theme = EditorView.theme({
  "&": {
    backgroundColor: "var(--duct-surface)",
    color: "var(--duct-text)",
    height: "100%",
    fontSize: "0.875rem",
  },
  "&.cm-focused": { outline: "2px solid var(--duct-orange)", outlineOffset: "1px" },
  ".cm-scroller": { fontFamily: "var(--font-mono)", overflow: "auto" },
  ".cm-gutters": {
    backgroundColor: "var(--duct-surface-2)",
    color: "var(--duct-muted)",
    border: "none",
    borderRadius: "0.5rem 0 0 0.5rem",
  },
  ".cm-activeLine, .cm-activeLineGutter": { backgroundColor: "var(--duct-hover)" },
  ".cm-cursor": { borderLeftColor: "var(--duct-orange)" },
  "&.cm-focused .cm-selectionBackground, .cm-selectionBackground, ::selection": {
    backgroundColor: "var(--duct-selection)",
  },
  ".cm-tooltip": {
    backgroundColor: "var(--duct-surface)",
    color: "var(--duct-text)",
    border: "1px solid var(--duct-border)",
  },
});

// ---- catalog (autocompletion source) ----

// One shared config object: the schema completion of lang-sql is built when
// the sql() extension is created, so a later fetch re-runs sql() and
// reconfigures the editor (see mount).
const SQL_CONFIG = { dialect: StandardSQL, upperCaseKeywords: true, schema: {} };
let catalogPromise = null;

function buildSchemaObject(cat) {
  const out = {};
  for (const s of cat.schemas || []) {
    const so = (out[s.name] = {});
    for (const t of s.tables || []) {
      so[t.name] = (t.columns || []).map((c) => ({ label: c.name, type: "property", detail: c.type }));
    }
  }
  return out;
}

function loadCatalog() {
  if (!catalogPromise) {
    catalogPromise = fetch("/catalog.json", { headers: { Accept: "application/json" } })
      .then((r) => (r.ok ? r.json() : null))
      .then((cat) => {
        if (cat && cat.schemas) SQL_CONFIG.schema = buildSchemaObject(cat);
        return true;
      })
      .catch(() => false);
  }
  return catalogPromise;
}

// ---- identifiers ----

// Reserved words that must be quoted even though they look like plain
// identifiers (DuckDB); everything else is quoted only when it does not
// match [a-z_][a-z0-9_]*.
const RESERVED = new Set(
  ("all and any as asc between by case cast check column constraint create cross current_date current_time " +
    "current_timestamp default delete desc distinct drop else end escape exists false for foreign from group " +
    "having in index inner insert intersect interval into is join key left like limit not null offset on or " +
    "order outer primary references right select set table then to true union unique update using values " +
    "view when whenever where with")
    .split(" ")
    .filter(Boolean)
);

function qident(name) {
  if (/^[a-z_][a-z0-9_]*$/.test(name) && !RESERVED.has(name)) return name;
  return '"' + String(name).replace(/"/g, '""') + '"';
}

function tableRef(row) {
  return qident(row.dataset.schema) + "." + qident(row.dataset.table);
}

// tableColumns reads the column rows of a table from the rendered tree.
function tableColumns(tree, schema, table) {
  const head = tree.querySelector(
    '[data-obj="table"][data-schema="' + CSS.escape(schema) + '"][data-table="' + CSS.escape(table) + '"]'
  );
  if (!head || !head.parentElement) return [];
  return Array.prototype.map.call(
    head.parentElement.querySelectorAll('[data-obj="column"]'),
    (r) => r.dataset.col
  );
}

function selectAllSql(cols, ref) {
  if (!cols.length) return "SELECT * FROM " + ref + " LIMIT 100";
  return "SELECT " + cols.map(qident).join(", ") + " FROM " + ref + " LIMIT 100";
}

// ---- editor actions ----

// pad keeps identifiers apart: dropping "t" next to a word would glue them.
function pad(view, from, to, text) {
  const doc = view.state.doc;
  const before = from > 0 ? doc.sliceString(from - 1, from) : "";
  const after = to < doc.length ? doc.sliceString(to, to + 1) : "";
  if (/[\w"]/.test(before) && /^[\w"*]/.test(text)) text = " " + text;
  if (/[\w"]/.test(after) && /[\w"*]$/.test(text)) text = text + " ";
  return text;
}

function insertAtCursor(view, text) {
  const { from, to } = view.state.selection.main;
  text = pad(view, from, to, text);
  view.dispatch({
    changes: { from, to, insert: text },
    selection: { anchor: from + text.length },
    scrollIntoView: true,
  });
  view.focus();
  return { from, len: text.length, text };
}

function copyText(text) {
  // Reuses the [data-copy-text] handler of shell.js (clipboard + toast).
  const b = document.createElement("button");
  b.type = "button";
  b.setAttribute("data-copy-text", text);
  b.style.display = "none";
  document.body.appendChild(b);
  b.click();
  b.remove();
}

// previewTable runs "SELECT * … LIMIT 100" in the results panel without
// touching the worksheet: it rides the selection signal (the server prefers
// sel over sql) and marks the run as silent so it stays out of the history.
function previewTable(schema, table) {
  const selInput = document.querySelector("[data-bind\\:sel], [data-sql-sel]");
  if (selInput) {
    selInput.value = "SELECT * FROM " + tableRef({ dataset: { schema, table } }) + " LIMIT 100";
    selInput.dispatchEvent(new Event("input", { bubbles: true }));
  }
  window.ductSilentRun = true;
  const tab = document.getElementById("tab-result");
  if (tab && tab.getAttribute("aria-selected") !== "true") tab.click();
  const run = document.getElementById("run-btn");
  if (run) run.click();
}

// ---- context menu ----

let menuEl = null;
function hideMenu() {
  if (menuEl) {
    menuEl.remove();
    menuEl = null;
  }
}
function showMenu(x, y, items) {
  hideMenu();
  const menu = document.createElement("div");
  menu.className = "ctx-menu";
  menu.setAttribute("role", "menu");
  items.forEach((it) => {
    const b = document.createElement("button");
    b.type = "button";
    b.className = "menu-item";
    b.setAttribute("role", "menuitem");
    b.textContent = it.label;
    b.addEventListener("click", () => {
      hideMenu();
      it.run();
    });
    menu.appendChild(b);
  });
  document.body.appendChild(menu);
  const r = menu.getBoundingClientRect();
  menu.style.left = Math.max(4, Math.min(x, window.innerWidth - r.width - 8)) + "px";
  menu.style.top = Math.max(4, Math.min(y, window.innerHeight - r.height - 8)) + "px";
  menuEl = menu;
  menu.addEventListener("keydown", (e) => {
    const btns = Array.prototype.slice.call(menu.querySelectorAll("button"));
    const i = btns.indexOf(document.activeElement);
    if (e.key === "Escape") {
      e.preventDefault();
      hideMenu();
    } else if (e.key === "ArrowDown" || e.key === "ArrowUp") {
      e.preventDefault();
      const n = (i + (e.key === "ArrowDown" ? 1 : -1) + btns.length) % btns.length;
      btns[n].focus();
    }
  });
  const first = menu.querySelector("button");
  if (first) first.focus();
}

// ---- drag & drop payload ----

function columnPayload(row) {
  return { schema: row.dataset.schema, table: row.dataset.table, column: row.dataset.col };
}

function dragPayload(tree, row) {
  if (row.dataset.obj === "table") {
    return { kind: "table", schema: row.dataset.schema, table: row.dataset.table };
  }
  const selected = tree.querySelectorAll('[data-obj="column"][data-selected="true"]');
  if (row.hasAttribute("data-selected") && selected.length > 1) {
    return { kind: "columns", items: Array.prototype.map.call(selected, columnPayload) };
  }
  return Object.assign({ kind: "column" }, columnPayload(row));
}

function payloadLabel(tree, p) {
  if (p.kind === "table") return p.schema + "." + p.table;
  if (p.kind === "column") return p.column;
  const names = Array.prototype.map.call(
    tree.querySelectorAll('[data-obj="column"][data-selected="true"]'),
    (r) => r.dataset.col
  );
  if (names.length > 1) return names.slice(0, 3).join(", ") + (names.length > 3 ? "…" : "") + " (" + names.length + ")";
  return p.items && p.items[0] ? p.items[0].column : "";
}

// dropText builds what lands in the editor: a ready query when dropped on an
// empty document or line (or the plain qualified name with Alt held, or in
// the middle of an existing statement).
function dropText(tree, p, full) {
  if (p.kind === "table") {
    const ref = qident(p.schema) + "." + qident(p.table);
    return full ? selectAllSql(tableColumns(tree, p.schema, p.table), ref) : ref;
  }
  if (p.kind === "column") {
    const ref = qident(p.schema) + "." + qident(p.table);
    return full ? "SELECT " + qident(p.column) + " FROM " + ref + " LIMIT 100" : qident(p.column);
  }
  const items = p.items || [];
  if (!items.length) return "";
  const same = items.every((i) => i.schema === items[0].schema && i.table === items[0].table);
  const names = items.map((i) => qident(i.column));
  if (full && same) {
    const ref = qident(items[0].schema) + "." + qident(items[0].table);
    return "SELECT " + names.join(", ") + " FROM " + ref + " LIMIT 100";
  }
  return names.join(", ");
}

function hasDragPayload(dt) {
  if (!dt) return false;
  const types = dt.types;
  return !!types && Array.prototype.indexOf.call(types, "application/json") >= 0;
}

// The drop caret: a thin orange bar at the position the drop would land on
// (CodeMirror's own dropCursor only follows its internal drags).
let caretEl = null;
function hideDropCaret() {
  if (caretEl) {
    caretEl.remove();
    caretEl = null;
  }
}
function showDropCaret(view, e) {
  const pos = view.posAtCoords({ x: e.clientX, y: e.clientY }, false);
  const c = pos == null ? null : view.coordsAtPos(pos);
  if (!c) return hideDropCaret();
  if (!caretEl) {
    caretEl = document.createElement("div");
    caretEl.className = "duct-drop-caret";
    document.body.appendChild(caretEl);
  }
  caretEl.style.left = c.left - 1 + "px";
  caretEl.style.top = c.top + "px";
  caretEl.style.height = c.bottom - c.top + "px";
}

function dropInsert(view, tree, e) {
  let p = null;
  try {
    p = JSON.parse(e.dataTransfer.getData("application/json"));
  } catch (err) {
    return false;
  }
  if (!p || !p.kind) return false;
  e.preventDefault();
  const pos = view.posAtCoords({ x: e.clientX, y: e.clientY }, false);
  if (pos == null) return true;
  const doc = view.state.doc;
  const line = doc.lineAt(pos);
  const emptyLine = line.text.trim() === "";
  const full = (doc.length === 0 || emptyLine) && !e.altKey;
  let text = dropText(tree, p, full);
  if (text) text = pad(view, pos, pos, text);
  if (text) {
    view.dispatch({
      changes: { from: pos, insert: text },
      selection: { anchor: pos + text.length },
      scrollIntoView: true,
    });
  }
  view.focus();
  return true;
}

// ---- tree wiring (click / double click / context menu / drag) ----

function wireTree(tree, view) {
  let ghost = null;
  let lastInsert = null; // { row, from, len, text, t } — undo on double-click

  tree.addEventListener("dragstart", (e) => {
    const row = e.target && e.target.closest ? e.target.closest("[data-obj]") : null;
    if (!row || !e.dataTransfer) return;
    const p = dragPayload(tree, row);
    const label = payloadLabel(tree, p);
    e.dataTransfer.effectAllowed = "copy";
    e.dataTransfer.setData("application/json", JSON.stringify(p));
    e.dataTransfer.setData("text/plain", label);
    ghost = document.createElement("div");
    ghost.className = "duct-drag-ghost";
    ghost.textContent = label;
    document.body.appendChild(ghost);
    e.dataTransfer.setDragImage(ghost, 12, 12);
    row.setAttribute("data-dragging", "true");
  });
  tree.addEventListener("dragend", () => {
    hideDropCaret();
    tree.querySelectorAll("[data-dragging]").forEach((n) => n.removeAttribute("data-dragging"));
    if (ghost) {
      ghost.remove();
      ghost = null;
    }
  });

  tree.addEventListener("click", (e) => {
    const row = e.target && e.target.closest ? e.target.closest("[data-tree-row]") : null;
    if (!row || !tree.contains(row)) return;
    const obj = row.dataset.obj;
    if (!obj) return; // schema row: expand/collapse natively
    if (e.target.closest("[data-chev]")) return; // expander: toggle natively
    e.preventDefault();
    if (obj === "table") {
      // A second click within the double-click window is handled by dblclick.
      if (lastInsert && lastInsert.row === row && Date.now() - lastInsert.t < 700) return;
      tree.querySelectorAll('[data-obj="column"][data-selected="true"]').forEach((n) => {
        n.removeAttribute("data-selected");
      });
      lastInsert = { row, t: Date.now() };
      Object.assign(lastInsert, insertAtCursor(view, tableRef(row)));
    } else if (obj === "column") {
      if (e.ctrlKey || e.metaKey) {
        if (row.hasAttribute("data-selected")) row.removeAttribute("data-selected");
        else row.setAttribute("data-selected", "true");
        return;
      }
      // Second click of a double click: keep a single insert (the dblclick
      // handler only previews tables).
      if (lastInsert && lastInsert.row === row && Date.now() - lastInsert.t < 700) return;
      const selected = tree.querySelectorAll('[data-obj="column"][data-selected="true"]');
      if (row.hasAttribute("data-selected") && selected.length > 1) {
        const list = Array.prototype.map.call(selected, (r) => qident(r.dataset.col));
        lastInsert = { row, t: Date.now() };
        Object.assign(lastInsert, insertAtCursor(view, list.join(", ")));
      } else {
        lastInsert = { row, t: Date.now() };
        Object.assign(lastInsert, insertAtCursor(view, qident(row.dataset.col)));
      }
    }
  });

  tree.addEventListener("dblclick", (e) => {
    const row = e.target && e.target.closest ? e.target.closest("[data-tree-row]") : null;
    if (!row || row.dataset.obj !== "table" || e.target.closest("[data-chev]")) return;
    e.preventDefault();
    if (
      lastInsert &&
      lastInsert.row === row &&
      Date.now() - lastInsert.t < 700 &&
      view.state.sliceDoc(lastInsert.from, lastInsert.from + lastInsert.len) === lastInsert.text
    ) {
      view.dispatch({
        changes: { from: lastInsert.from, to: lastInsert.from + lastInsert.len, insert: "" },
        selection: { anchor: lastInsert.from },
      });
    }
    lastInsert = null;
    previewTable(row.dataset.schema, row.dataset.table);
  });

  tree.addEventListener("contextmenu", (e) => {
    const row = e.target && e.target.closest ? e.target.closest("[data-tree-row][data-obj]") : null;
    if (!row || !tree.contains(row)) return;
    e.preventDefault();
    const isTable = row.dataset.obj === "table";
    const items = isTable
      ? [
          {
            label: "SELECT * FROM",
            run: () =>
              insertAtCursor(
                view,
                "SELECT * FROM " + tableRef(row) + " LIMIT 100"
              ),
          },
          {
            label: "SELECT columnas",
            run: () =>
              insertAtCursor(
                view,
                selectAllSql(
                  tableColumns(tree, row.dataset.schema, row.dataset.table),
                  tableRef(row)
                )
              ),
          },
          { label: "Copiar nombre", run: () => copyText(row.dataset.schema + "." + row.dataset.table) },
          { label: "Vista previa", run: () => previewTable(row.dataset.schema, row.dataset.table) },
        ]
      : [
          { label: "Insertar", run: () => insertAtCursor(view, qident(row.dataset.col)) },
          { label: "Copiar nombre", run: () => copyText(row.dataset.col) },
        ];
    showMenu(e.clientX, e.clientY, items);
  });
}

document.addEventListener(
  "click",
  (e) => {
    // Clicks inside the menu are handled by the item itself.
    if (menuEl && menuEl.contains(e.target)) return;
    hideMenu();
  },
  true
);
// Capture: closes a previous menu BEFORE the tree opens a new one.
document.addEventListener(
  "contextmenu",
  (e) => {
    if (menuEl && menuEl.contains(e.target)) return;
    hideMenu();
  },
  true
);
document.addEventListener("keydown", (e) => {
  if (e.key === "Escape") hideMenu();
});

function mount(host) {
  const area = host.parentElement.querySelector("textarea");
  if (!area || host.dataset.ready) return;
  host.dataset.ready = "1";
  const tree = document.querySelector("[data-tree]");
  const sync = (text) => {
    if (area.value === text) return;
    area.value = text;
    area.dispatchEvent(new Event("input", { bubbles: true }));
  };
  const run = () => {
    const btn = document.querySelector(host.dataset.run || "");
    if (btn) btn.click();
    return true;
  };
  const dnd = EditorView.domEventHandlers({
    dragover(e) {
      if (!hasDragPayload(e.dataTransfer)) return false;
      e.preventDefault();
      e.dataTransfer.dropEffect = "copy";
      showDropCaret(view, e);
      return true;
    },
    dragleave(e) {
      if (!view.dom.contains(e.relatedTarget)) hideDropCaret();
      return false;
    },
    drop(e) {
      hideDropCaret();
      if (!tree || !hasDragPayload(e.dataTransfer)) return false;
      return dropInsert(view, tree, e);
    },
  });
  const sqlExt = sql(SQL_CONFIG);
  const extensions = [
    keymap.of([{ key: "Mod-Enter", run }]),
    basicSetup,
    sqlExt,
    theme,
    syntaxHighlighting(highlight),
    dnd,
    EditorView.updateListener.of((u) => {
      if (u.docChanged) sync(u.state.doc.toString());
      if (u.docChanged || u.selectionSet) {
        document.dispatchEvent(new CustomEvent("duct:sql-selection"));
      }
    }),
  ];
  const view = new EditorView({
    parent: host,
    state: EditorState.create({ doc: area.value, extensions }),
  });
  area.hidden = true;

  if (tree) wireTree(tree, view);

  // Catalog arrives after the editor: rebuild the sql() extension so the
  // schema completion (built eagerly by lang-sql) sees the role's objects.
  loadCatalog().then((ok) => {
    if (!ok) return;
    extensions[extensions.indexOf(sqlExt)] = sql(SQL_CONFIG);
    try {
      view.dispatch({ effects: StateEffect.reconfigure.of(extensions) });
      window.ductEditorSchemaReady = true;
    } catch (err) {
      console.error("duct: sql reconfigure failed", err);
    }
  });
  // Introspection for the browser tests (same data as /catalog.json).
  window.ductEditorCatalog = () => SQL_CONFIG.schema;
  window.ductEditorView = view;

  // Contract with shell.js: set/insert SQL and read the current selection
  // (run selection or all is decided from it).
  window.ductEditorSelection = () => {
    const { main } = view.state.selection;
    return main.empty ? "" : view.state.sliceDoc(main.from, main.to);
  };
  document.addEventListener("duct:sql-set", (e) => {
    const text = String(e.detail ?? "");
    if (view.state.doc.toString() === text) return;
    view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text }, selection: { anchor: 0 } });
    view.focus();
  });
  document.addEventListener("duct:sql-insert", (e) => {
    const text = String(e.detail ?? "");
    insertAtCursor(view, text);
  });
}

document.querySelectorAll("[data-sql-editor]").forEach(mount);
