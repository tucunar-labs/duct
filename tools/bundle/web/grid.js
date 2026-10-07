// grid.js: the results island of the SQL editor and the catalog preview
// (ADR 0012). A tiny custom element that reads the compact JSON payload of
// the SSE patch (data-spec: columns + rows arrays) and renders it with
// gridtable.js — row virtualization, sticky header, type badges, CSV export.
// The server replaces the results container (patch mode "replace"), so no
// large subtree is ever morphed into the DOM.
import { mountGrid } from "./gridtable.js";

class DuctGrid extends HTMLElement {
  static get observedAttributes() {
    return ["data-spec"];
  }

  connectedCallback() {
    this.render();
  }

  attributeChangedCallback(name) {
    if (name === "data-spec" && this.isConnected) this.render();
  }

  render() {
    const raw = this.getAttribute("data-spec");
    if (!raw) return;
    let spec;
    try {
      spec = JSON.parse(raw);
    } catch (e) {
      this.textContent = "Respuesta inválida";
      return;
    }
    if (this.__controller) this.__controller.destroy();
    this.__spec = spec;
    this.__controller = mountGrid(this, spec, { label: "Resultados de la consulta" });
  }

  // exportCSV downloads the full result (not just the rows in the DOM).
  exportCSV() {
    if (this.__controller && this.__spec) this.__controller.exportCSV();
  }
}

if (!customElements.get("duct-grid")) customElements.define("duct-grid", DuctGrid);
