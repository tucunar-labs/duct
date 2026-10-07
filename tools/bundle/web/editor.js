// CodeMirror 6 island for the Duct SQL editor (ADR 0012).
//
// Contract with the page: a <div data-sql-editor> next to a <textarea> that
// holds the SQL. The editor mirrors its content into the textarea and fires
// an `input` event so Datastar's data-bind keeps the signal in sync.
// Mod-Enter clicks the element matching the data-run selector.
import { basicSetup } from "codemirror";
import { EditorView, keymap } from "@codemirror/view";
import { EditorState } from "@codemirror/state";
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
    border: "1px solid var(--duct-border)",
    borderRadius: "0.5rem",
    minHeight: "11rem",
    maxHeight: "28rem",
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

function mount(host) {
  const area = host.parentElement.querySelector("textarea");
  if (!area || host.dataset.ready) return;
  host.dataset.ready = "1";
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
  new EditorView({
    parent: host,
    state: EditorState.create({
      doc: area.value,
      extensions: [
        keymap.of([{ key: "Mod-Enter", run }]),
        basicSetup,
        sql({ dialect: StandardSQL, upperCaseKeywords: true }),
        theme,
        syntaxHighlighting(highlight),
        EditorView.updateListener.of((u) => u.docChanged && sync(u.state.doc.toString())),
      ],
    }),
  });
  area.hidden = true;
}

document.querySelectorAll("[data-sql-editor]").forEach(mount);
