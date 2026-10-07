// shell.js: the app-shell island of the Duct web UI (ADR 0012). Plain DOM,
// no framework: theme (light/dark/system), sidebar collapse, menus, toasts,
// tree search, tabs, the SQL editor splitter, worksheets + query history
// (localStorage) and results-grid helpers (copy cell/row, export CSV).
//
// It is a classic (non-module) script loaded in <head> so the theme is
// applied before first paint. CSP: no inline script, no eval.
(function () {
  "use strict";

  // ---- theme (runs before body: no flash) ----
  var THEME_KEY = "duct.theme";
  var SIDEBAR_KEY = "duct.sidebar";

  function storedTheme() {
    try {
      return localStorage.getItem(THEME_KEY) || "";
    } catch (e) {
      return "";
    }
  }
  function applyTheme(t) {
    var el = document.documentElement;
    if (t) {
      el.setAttribute("data-theme", t);
    } else {
      el.removeAttribute("data-theme");
    }
    el.setAttribute("data-theme-mode", t || "system");
  }
  applyTheme(storedTheme());

  function applySidebar(s) {
    if (s) {
      document.documentElement.setAttribute("data-sidebar", s);
    } else {
      document.documentElement.removeAttribute("data-sidebar");
    }
  }
  try {
    applySidebar(localStorage.getItem(SIDEBAR_KEY) || "");
  } catch (e) {}

  // ---- small helpers ----
  function $(sel, root) {
    return (root || document).querySelector(sel);
  }
  function $$(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }
  function on(el, ev, fn, opts) {
    if (el) el.addEventListener(ev, fn, opts);
  }
  function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).catch(function () {
        fallbackCopy(text);
      });
    } else {
      fallbackCopy(text);
    }
  }
  function fallbackCopy(text) {
    var ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
    } catch (e) {}
    document.body.removeChild(ta);
  }

  // ---- toasts ----
  var toastBox = null;
  function toast(msg, kind) {
    if (!toastBox) {
      toastBox = document.createElement("div");
      toastBox.id = "toasts";
      toastBox.setAttribute("aria-live", "polite");
      toastBox.style.cssText =
        "position:fixed;right:1rem;bottom:1rem;z-index:80;display:flex;flex-direction:column;gap:0.5rem;align-items:flex-end";
      document.body.appendChild(toastBox);
    }
    var t = document.createElement("div");
    t.className = "toast" + (kind === "error" ? " badge-danger" : "");
    t.textContent = msg;
    toastBox.appendChild(t);
    setTimeout(function () {
      t.remove();
    }, 2600);
  }
  window.ductToast = toast;

  // ---- generic toggles ----
  function wire() {
    // Theme cycle: system -> light -> dark -> system.
    $$("[data-theme-toggle]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var order = ["", "light", "dark"];
        var next = order[(order.indexOf(storedTheme()) + 1) % order.length];
        try {
          if (next) localStorage.setItem(THEME_KEY, next);
          else localStorage.removeItem(THEME_KEY);
        } catch (e) {}
        applyTheme(next);
        toast(next === "" ? "Tema: sistema" : next === "dark" ? "Tema: oscuro" : "Tema: claro");
      });
    });

    // Sidebar collapse.
    $$("[data-sidebar-toggle]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var cur = document.documentElement.getAttribute("data-sidebar") === "collapsed" ? "" : "collapsed";
        applySidebar(cur);
        try {
          if (cur) localStorage.setItem(SIDEBAR_KEY, cur);
          else localStorage.removeItem(SIDEBAR_KEY);
        } catch (e) {}
      });
    });

    // Dropdown menus (user menu).
    $$("[data-menu]").forEach(function (wrap) {
      var toggle = $("[data-menu-toggle]", wrap);
      var panel = $("[data-menu-panel]", wrap);
      if (!toggle || !panel) return;
      function setOpen(open) {
        panel.hidden = !open;
        toggle.setAttribute("aria-expanded", open ? "true" : "false");
      }
      on(toggle, "click", function (e) {
        e.stopPropagation();
        setOpen(panel.hidden);
        if (!panel.hidden) {
          var first = $("button, a, input, select", panel);
          if (first) first.focus();
        }
      });
      on(document, "click", function (e) {
        if (!wrap.contains(e.target)) setOpen(false);
      });
      on(document, "keydown", function (e) {
        if (e.key === "Escape" && !panel.hidden) {
          setOpen(false);
          toggle.focus();
        }
      });
    });

    // Tree search (catalog explorer + SQL schema browser).
    $$("[data-tree-search]").forEach(function (input) {
      var root = document.querySelector(input.getAttribute("data-tree-search"));
      var empty = root ? $("[data-tree-empty]", root.parentElement || document) : null;
      if (!root) return;
      input.addEventListener("input", function () {
        filterTree(root, input.value, empty);
      });
    });

    // Tree keyboard navigation (roving tabindex): arrows move, Right/Left
    // expand/collapse, Enter/Space activates the row (link, button or the
    // insert handler of the SQL schema browser).
    $$("[data-tree]").forEach(function (tree) {
      function rows() {
        return $$("[data-tree-row]", tree).filter(function (r) {
          return !r.hidden && !r.closest("[hidden]");
        });
      }
      function focusRow(r) {
        if (!r) return;
        rows().forEach(function (o) {
          o.tabIndex = o === r ? 0 : -1;
        });
        r.focus();
        r.scrollIntoView({ block: "nearest" });
      }
      var first = rows()[0];
      if (first) first.tabIndex = 0;
      tree.addEventListener("focusin", function (e) {
        var r = e.target.closest ? e.target.closest("[data-tree-row]") : null;
        if (r) {
          rows().forEach(function (o) {
            o.tabIndex = o === r ? 0 : -1;
          });
        }
      });
      tree.addEventListener("keydown", function (e) {
        var list = rows();
        if (!list.length) return;
        var cur = document.activeElement && document.activeElement.closest ? document.activeElement.closest("[data-tree-row]") : null;
        var i = list.indexOf(cur);
        var d = e.key === "ArrowDown" ? 1 : e.key === "ArrowUp" ? -1 : 0;
        if (d) {
          e.preventDefault();
          focusRow(list[(i + d + list.length) % list.length]);
          return;
        }
        if (e.key === "Home") {
          e.preventDefault();
          focusRow(list[0]);
          return;
        }
        if (e.key === "End") {
          e.preventDefault();
          focusRow(list[list.length - 1]);
          return;
        }
        if (!cur) return;
        if (e.key === "ArrowRight") {
          var det = cur.tagName === "SUMMARY" ? cur.parentElement : null;
          if (det && det.tagName === "DETAILS" && !det.open) {
            e.preventDefault();
            det.open = true;
          } else {
            e.preventDefault();
            focusRow(list[i + 1]);
          }
          return;
        }
        if (e.key === "ArrowLeft") {
          var own = cur.tagName === "SUMMARY" ? cur.parentElement : null;
          if (own && own.tagName === "DETAILS" && own.open) {
            e.preventDefault();
            own.open = false;
          } else {
            var owner = cur.closest("details");
            var sum = owner ? owner.querySelector(":scope > summary") : null;
            if (sum && sum !== cur) {
              e.preventDefault();
              focusRow(sum);
            }
          }
          return;
        }
        if (e.key === "Enter" || e.key === " ") {
          // Explicit activation: Blink does not dispatch a click for Enter on
          // <summary>, and the SQL editor inserts on click, not on toggle.
          e.preventDefault();
          var act = cur.querySelector("a, button:not([disabled])");
          (act || cur).click();
        }
      });
    });

    // Tabs (catalog detail, results panes).
    $$("[data-tabs]").forEach(function (list) {
      var tabs = $$("[role=tab]", list);
      function select(tab) {
        tabs.forEach(function (t) {
          var on = t === tab;
          t.setAttribute("aria-selected", on ? "true" : "false");
          t.tabIndex = on ? 0 : -1;
          var panel = document.getElementById(t.getAttribute("aria-controls") || "");
          if (panel) panel.hidden = !on;
        });
      }
      tabs.forEach(function (tab, i) {
        tab.addEventListener("click", function () {
          select(tab);
        });
        tab.addEventListener("keydown", function (e) {
          var d = e.key === "ArrowRight" ? 1 : e.key === "ArrowLeft" ? -1 : e.key === "Home" ? -99 : e.key === "End" ? 99 : 0;
          if (!d) return;
          e.preventDefault();
          var n = d === -99 ? 0 : d === 99 ? tabs.length - 1 : (i + d + tabs.length) % tabs.length;
          select(tabs[n]);
          tabs[n].focus();
        });
      });
    });

    // Resizable splitter (editor | results).
    $$("[data-splitter]").forEach(function (handle) {
      var pane = $(handle.getAttribute("data-split"));
      if (!pane) return;
      var box = handle.parentElement;
      var saved = parseFloat(pane.getAttribute("data-split-default") || "0");
      try {
        var v = parseFloat(localStorage.getItem("duct.split") || "");
        if (v > 0) saved = v;
      } catch (e) {}
      function setH(h) {
        pane.style.flex = "none";
        pane.style.height = h + "px";
      }
      if (saved > 0) setH(saved);
      var dragging = false;
      on(handle, "pointerdown", function (e) {
        dragging = true;
        handle.setAttribute("data-dragging", "true");
        handle.setPointerCapture(e.pointerId);
        document.body.style.userSelect = "none";
        document.body.style.cursor = "row-resize";
      });
      on(handle, "pointermove", function (e) {
        if (!dragging) return;
        var top = box.getBoundingClientRect().top;
        var h = Math.min(Math.max(e.clientY - top, 96), box.clientHeight - 120);
        setH(h);
      });
      function stop() {
        if (!dragging) return;
        dragging = false;
        handle.removeAttribute("data-dragging");
        document.body.style.userSelect = "";
        document.body.style.cursor = "";
        try {
          localStorage.setItem("duct.split", String(Math.round(pane.getBoundingClientRect().height)));
        } catch (e) {}
      }
      on(handle, "pointerup", stop);
      on(handle, "pointercancel", stop);
      on(handle, "keydown", function (e) {
        var d = e.key === "ArrowUp" ? -24 : e.key === "ArrowDown" ? 24 : 0;
        if (!d) return;
        e.preventDefault();
        var h = Math.min(Math.max(pane.getBoundingClientRect().height + d, 96), box.clientHeight - 120);
        setH(h);
        try {
          localStorage.setItem("duct.split", String(Math.round(h)));
        } catch (e) {}
      });
    });

    // Vertical splitter: width of the SQL schema panel (persisted as a CSS
    // variable so the lg: media query keeps the small-screen layout).
    var SCHEMA_W = "duct.schemaWidth";
    $$("[data-splitter-v]").forEach(function (handle) {
      var pane = $(handle.getAttribute("data-split"));
      if (!pane) return;
      var box = handle.parentElement;
      var w = parseFloat(pane.getAttribute("data-split-default") || "288");
      try {
        var v = parseFloat(localStorage.getItem(SCHEMA_W) || "");
        if (v > 0) w = v;
      } catch (e) {}
      function setW(x) {
        pane.style.setProperty("--schema-w", x + "px");
      }
      function persist(x) {
        try {
          localStorage.setItem(SCHEMA_W, String(Math.round(x)));
        } catch (e) {}
      }
      setW(w);
      var dragging = false;
      on(handle, "pointerdown", function (e) {
        dragging = true;
        handle.setAttribute("data-dragging", "true");
        handle.setPointerCapture(e.pointerId);
        document.body.style.userSelect = "none";
        document.body.style.cursor = "col-resize";
      });
      on(handle, "pointermove", function (e) {
        if (!dragging) return;
        var left = box.getBoundingClientRect().left;
        var max = Math.min(560, box.clientWidth - 240);
        var x = Math.min(Math.max(e.clientX - left, 200), Math.max(200, max));
        setW(x);
      });
      function stop() {
        if (!dragging) return;
        dragging = false;
        handle.removeAttribute("data-dragging");
        document.body.style.userSelect = "";
        document.body.style.cursor = "";
        var w2 = pane.getBoundingClientRect().width;
        if (w2 > 0) persist(w2);
      }
      on(handle, "pointerup", stop);
      on(handle, "pointercancel", stop);
      on(handle, "keydown", function (e) {
        var d = e.key === "ArrowLeft" ? -24 : e.key === "ArrowRight" ? 24 : 0;
        if (!d) return;
        e.preventDefault();
        var cur = pane.getBoundingClientRect().width || w;
        var next = Math.min(Math.max(cur + d, 200), 560);
        setW(next);
        persist(next);
      });
    });

    // Collapse the schema panel (SQL editor); state persists per browser.
    $$("[data-schema-collapse]").forEach(function (btn) {
      var row = $("[data-schema-pane]");
      if (!row) return;
      function apply(collapsed) {
        if (collapsed) row.setAttribute("data-collapsed", "");
        else row.removeAttribute("data-collapsed");
        btn.setAttribute("aria-expanded", collapsed ? "false" : "true");
      }
      var stored = "";
      try {
        stored = localStorage.getItem("duct.schemaPane") || "";
      } catch (e) {}
      apply(stored === "collapsed");
      btn.addEventListener("click", function () {
        var collapsed = !row.hasAttribute("data-collapsed");
        apply(collapsed);
        try {
          if (collapsed) localStorage.setItem("duct.schemaPane", "collapsed");
          else localStorage.removeItem("duct.schemaPane");
        } catch (e) {}
      });
    });

    // Copy helpers: [data-copy-text] buttons, [data-copy] cells, [data-copy-row].
    on(document, "click", function (e) {
      var t = e.target.closest ? e.target.closest("[data-copy-text]") : null;
      if (t) {
        copyText(t.getAttribute("data-copy-text") || "");
        toast("Copiado");
        return;
      }
      var cell = e.target.closest ? e.target.closest("[data-copy]") : null;
      if (cell) {
        var sel = window.getSelection();
        if (sel && String(sel).length > 0) return; // the user is selecting text
        copyText(cell.textContent);
        cell.setAttribute("data-copied", "true");
        setTimeout(function () {
          cell.removeAttribute("data-copied");
        }, 900);
        toast("Celda copiada");
        return;
      }
      var rowBtn = e.target.closest ? e.target.closest("[data-copy-row]") : null;
      if (rowBtn) {
        var tr = rowBtn.closest("tr, .dgrid-row");
        if (!tr) return;
        var vals = $$("td[data-col], .dgrid-cell[data-col]", tr).map(function (td) {
          return td.classList.contains("results-null") || td.classList.contains("dgrid-null") ? "" : td.textContent;
        });
        copyText(vals.join("\t"));
        toast("Fila copiada");
      }
    });

    // Export CSV of the visible results grid (the <duct-grid> island holds
    // the full result; legacy <table> grids read the rendered cells).
    $$("[data-export-csv]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var res = $(btn.getAttribute("data-export-csv") || "[data-results]");
        var grid = res ? res.querySelector("duct-grid") : null;
        if (grid && grid.exportCSV) {
          grid.exportCSV();
          toast("CSV exportado");
          return;
        }
        var table = res ? $("table", res) : null;
        if (!table) {
          toast("No hay resultados que exportar", "error");
          return;
        }
        var cols = $$("thead th [data-col]", table).map(function (el) {
          return el.getAttribute("data-col");
        });
        var lines = [cols.map(csvCell).join(",")];
        $$("tbody tr", table).forEach(function (tr) {
          var vals = $$("td[data-col]", tr).map(function (td) {
            return td.classList.contains("results-null") ? "" : td.textContent;
          });
          lines.push(vals.map(csvCell).join(","));
        });
        download("duct-resultados.csv", lines.join("\r\n") + "\r\n", "text/csv;charset=utf-8");
        toast("CSV exportado");
      });
    });

    // SQL editor page: worksheets + history (only when present).
    var wsBar = $("[data-worksheets]");
    if (wsBar) wireSqlPage(wsBar);
    else wireHistory(null);

    // Result history recorder (also wired without a visible panel).
    recordResults();
  }

  function csvCell(v) {
    v = String(v == null ? "" : v);
    return /[",\r\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
  }

  function download(name, text, mime) {
    var blob = new Blob(["\ufeff" + text], { type: mime });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = name;
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function () {
      URL.revokeObjectURL(url);
    }, 4000);
  }

  // ---- tree filter ----
  function filterTree(root, qRaw, empty) {
    var q = qRaw.trim().toLowerCase();
    var nodes = $$("[data-tree-node]", root);
    var parent = new Map();
    var children = new Map();
    nodes.forEach(function (n) {
      var p = n.parentElement ? n.parentElement.closest("[data-tree-node]") : null;
      if (p && !root.contains(p)) p = null;
      parent.set(n, p);
      if (p) {
        if (!children.has(p)) children.set(p, []);
        children.get(p).push(n);
      }
    });
    var hit = new Map();
    var total = 0;
    // Reversed document order: descendants before ancestors.
    for (var i = nodes.length - 1; i >= 0; i--) {
      var n = nodes[i];
      var ok = q ? (n.getAttribute("data-search") || "").toLowerCase().indexOf(q) >= 0 : true;
      if (!ok) {
        (children.get(n) || []).forEach(function (c) {
          if (hit.get(c)) ok = true;
        });
      }
      hit.set(n, ok);
      n.hidden = !ok;
      if (ok && n.tagName === "DETAILS" && q) n.open = true;
    }
    if (!q) {
      nodes.forEach(function (n) {
        if (n.tagName === "DETAILS") n.open = n.hasAttribute("data-open");
      });
    }
    var any = false;
    hit.forEach(function (v) {
      if (v) any = true;
    });
    if (empty) empty.hidden = any;
  }

  // ---- worksheets + history (SQL page) ----
  var WS_KEY = "duct.worksheets";
  var WS_ACTIVE = "duct.worksheet.active";
  var HIST_KEY = "duct.history";
  var MAX_HIST = 100;

  function loadJSON(key, def) {
    try {
      var v = JSON.parse(localStorage.getItem(key));
      return v == null ? def : v;
    } catch (e) {
      return def;
    }
  }
  function saveJSON(key, v) {
    try {
      localStorage.setItem(key, JSON.stringify(v));
    } catch (e) {}
  }

  function wireSqlPage(wsBar) {
    var area = $("textarea[data-bind\\:sql], textarea[data-sql-signal]");
    var selInput = $("[data-bind\\:sel], [data-sql-sel]");
    var state = loadJSON(WS_KEY, null);
    var hadStored = Array.isArray(state) && state.length > 0;
    if (!hadStored) {
      state = [{ id: "w1", name: "Consulta 1", sql: area ? area.value : "" }];
    }
    var active = loadJSON(WS_ACTIVE, state[0].id);
    if (!state.some(function (w) {
      return w.id === active;
    }))
      active = state[0].id;

    // "Open in SQL editor" (?q=...): a fresh worksheet with that SQL; with no
    // saved worksheets the initial textarea already holds it, so keep one tab.
    var params = new URLSearchParams(location.search);
    if (params.has("q")) {
      if (hadStored) {
        var id = "w" + Date.now().toString(36);
        state.push({ id: id, name: "Consulta " + (state.length + 1), sql: area ? area.value : params.get("q") });
        active = id;
      }
      history.replaceState(null, "", location.pathname);
    }

    function current() {
      return (
        state.filter(function (w) {
          return w.id === active;
        })[0] || state[0]
      );
    }
    function persist() {
      saveJSON(WS_KEY, state);
      saveJSON(WS_ACTIVE, active);
    }
    function renderTabs() {
      wsBar.innerHTML = "";
      state.forEach(function (w) {
        var tab = document.createElement("button");
        tab.type = "button";
        tab.className = "ws-tab";
        tab.setAttribute("role", "tab");
        tab.setAttribute("aria-selected", w.id === active ? "true" : "false");
        tab.title = "Doble clic para renombrar";
        var label = document.createElement("span");
        label.className = "truncate";
        label.textContent = w.name;
        tab.appendChild(label);
        var close = document.createElement("span");
        close.setAttribute("aria-hidden", "true");
        close.textContent = "×";
        close.style.cssText = "opacity:0.6;padding:0 0.15rem";
        tab.appendChild(close);
        tab.addEventListener("click", function (e) {
          if (e.target === close) {
            e.stopPropagation();
            if (state.length === 1) {
              toast("Debe quedar al menos una consulta", "error");
              return;
            }
            state = state.filter(function (x) {
              return x.id !== w.id;
            });
            if (active === w.id) active = state[0].id;
            persist();
            renderTabs();
            apply();
            return;
          }
          active = w.id;
          persist();
          renderTabs();
          apply();
        });
        tab.addEventListener("dblclick", function () {
          var name = prompt("Nombre de la consulta", w.name);
          if (name) {
            w.name = name.slice(0, 60);
            persist();
            renderTabs();
          }
        });
        wsBar.appendChild(tab);
      });
      var add = document.createElement("button");
      add.type = "button";
      add.className = "btn btn-ghost btn-icon";
      add.setAttribute("aria-label", "Nueva consulta");
      add.title = "Nueva consulta";
      add.textContent = "+";
      add.addEventListener("click", function () {
        var id = "w" + Date.now().toString(36);
        state.push({ id: id, name: "Consulta " + (state.length + 1), sql: "" });
        active = id;
        persist();
        renderTabs();
        apply();
      });
      wsBar.appendChild(add);
    }
    function apply() {
      var w = current();
      if (area && area.value !== w.sql) {
        document.dispatchEvent(new CustomEvent("duct:sql-set", { detail: w.sql }));
        area.value = w.sql;
        area.dispatchEvent(new Event("input", { bubbles: true }));
      }
    }
    if (area) {
      var saveTimer = null;
      area.addEventListener("input", function () {
        clearTimeout(saveTimer);
        saveTimer = setTimeout(function () {
          current().sql = area.value;
          persist();
        }, 250);
      });
    }
    // Keep the run-selection signal fresh for the run button.
    if (selInput) {
      var syncSel = function () {
        var viewSel = window.ductEditorSelection ? window.ductEditorSelection() : "";
        if (selInput.value !== viewSel) {
          selInput.value = viewSel;
          selInput.dispatchEvent(new Event("input", { bubbles: true }));
        }
      };
      on(document, "keyup", syncSel);
      on(document, "mouseup", syncSel);
      on(document, "duct:sql-selection", syncSel);
    }
    renderTabs();
    if (!params.has("q")) apply();
    wireHistory(area);
  }

  function wireHistory(area) {
    var list = $("[data-history]");
    if (!list) return;
    function render() {
      var items = loadJSON(HIST_KEY, []);
      list.innerHTML = "";
      if (items.length === 0) {
        var e = document.createElement("p");
        e.className = "empty";
        e.innerHTML = '<span class="empty-title">Sin consultas todavía</span><span class="text-xs">Lo que ejecutes aparecerá aquí (solo en este navegador).</span>';
        list.appendChild(e);
        return;
      }
      items.forEach(function (h) {
        var li = document.createElement("button");
        li.type = "button";
        li.className = "block w-full rounded-lg border border-line bg-surface p-2 text-left hover:bg-hover";
        var head = document.createElement("div");
        head.className = "flex items-center gap-2 text-xs text-muted";
        var when = document.createElement("span");
        when.textContent = new Date(h.ts).toLocaleString();
        head.appendChild(when);
        var meta = document.createElement("span");
        meta.className = "badge " + (h.error ? "badge-danger" : "badge-muted");
        meta.textContent = h.error ? "error" : h.rows + " filas · " + h.ms + " ms";
        meta.style.marginLeft = "auto";
        head.appendChild(meta);
        li.appendChild(head);
        var q = document.createElement("div");
        q.className = "mt-1 truncate font-mono text-xs";
        q.textContent = (h.sql || "").trim().split("\n").slice(0, 2).join(" ");
        li.appendChild(q);
        if (h.error) {
          li.title = h.error;
        }
        li.addEventListener("click", function () {
          if (area) {
            document.dispatchEvent(new CustomEvent("duct:sql-set", { detail: h.sql }));
            area.value = h.sql;
            area.dispatchEvent(new Event("input", { bubbles: true }));
            toast("Consulta cargada en el editor");
          }
        });
        list.appendChild(li);
      });
    }
    $$("[data-history-clear]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        saveJSON(HIST_KEY, []);
        render();
        toast("Historial vaciado");
      });
    });
    render();
    window.ductHistoryRender = render;
    // The run button posts through Datastar; capture the SQL about to run.
    var run = $("#run-btn");
    on(run, "click", function () {
      if (window.ductSilentRun) {
        // Quick preview from the schema tree: run it, but keep it out of the
        // local history (recordResults clears the flag with the result).
        window.ductPendingRun = { ts: Date.now(), sql: "", sel: "" };
        return;
      }
      window.ductPendingRun = {
        ts: Date.now(),
        sql: area ? area.value : "",
        sel: (function () {
          var s = $("[data-bind\\:sel], [data-sql-sel]");
          return s ? s.value : "";
        })(),
      };
    });
  }

  // Records every result patch into the local query history. The results
  // container arrives as a replaced node (patch mode "replace"), so both
  // childList additions and attribute changes are watched; a unique nonce
  // per render keeps each result recorded exactly once.
  function recordResults() {
    var run = $("#run-btn");
    if (run && !window.ductPendingRun) {
      run.addEventListener("click", function () {
        if (window.ductSilentRun) {
          window.ductPendingRun = { ts: Date.now(), sql: "", sel: "" };
          return;
        }
        var area = $("textarea[data-bind\\:sql], textarea[data-sql-signal]");
        var sel = $("[data-bind\\:sel], [data-sql-sel]");
        window.ductPendingRun = { ts: Date.now(), sql: area ? area.value : "", sel: sel ? sel.value : "" };
      });
    }
    var host = $("[data-results-host]") || document.body;
    var lastNonce = null;
    function record(el) {
      if (!el || !el.getAttribute) return;
      var nonce = el.getAttribute("data-nonce");
      if (nonce == null || nonce === lastNonce) return;
      lastNonce = nonce;
      var p = window.ductPendingRun;
      window.ductPendingRun = null;
      window.ductSilentRun = false;
      var error = el.getAttribute("data-error") || "";
      var entry = {
        ts: p ? p.ts : Date.now(),
        sql: (p && (p.sel || p.sql)) || "",
        rows: parseInt(el.getAttribute("data-rows") || "0", 10),
        ms: Math.round(parseFloat(el.getAttribute("data-ms") || "0")),
        error: error,
      };
      if (!entry.sql) return;
      var items = loadJSON(HIST_KEY, []);
      items.unshift(entry);
      saveJSON(HIST_KEY, items.slice(0, MAX_HIST));
      if (window.ductHistoryRender) window.ductHistoryRender();
    }
    var obs = new MutationObserver(function (muts) {
      muts.forEach(function (m) {
        if (m.type === "attributes") {
          if (m.target.hasAttribute && m.target.hasAttribute("data-nonce")) record(m.target);
          return;
        }
        if (m.type === "childList" && m.addedNodes) {
          Array.prototype.forEach.call(m.addedNodes, function (n) {
            if (n.nodeType !== 1) return;
            if (n.hasAttribute("data-nonce")) record(n);
            else if (n.querySelector) record(n.querySelector("[data-nonce]"));
          });
        }
      });
    });
    obs.observe(host, {
      subtree: true,
      attributes: true,
      attributeFilter: ["data-nonce"],
      childList: true,
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", wire);
  } else {
    wire();
  }
})();
