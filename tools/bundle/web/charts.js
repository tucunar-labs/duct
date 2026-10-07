// <duct-chart>: the dashboard island (ADR 0008, 0012). The server (Go + Datastar)
// patches the element's `data-spec` attribute with a chart's rows and a limited,
// declarative set of options; this file turns them into an Apache ECharts option
// (or a KPI / table, which need no ECharts). Clicks and brushes are reported as a
// `ductcross` DOM event that Datastar turns into a cross-filter request; the
// browser never builds queries. The chart lives in a shadow root, so DOM morphs
// of the page never touch it.
import * as echarts from 'echarts/core';
import { BarChart, LineChart, PieChart, ScatterChart, HeatmapChart } from 'echarts/charts';
import {
  GridComponent,
  TooltipComponent,
  LegendComponent,
  VisualMapContinuousComponent,
  BrushComponent,
  ToolboxComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';

echarts.use([
  BarChart, LineChart, PieChart, ScatterChart, HeatmapChart,
  GridComponent, TooltipComponent, LegendComponent, VisualMapContinuousComponent,
  BrushComponent, ToolboxComponent,
  CanvasRenderer,
]);

// Brand palette (assets/brand/README.md): orange first, then a categorical set
// that stays distinguishable in light and dark and does not clash with it.
const THEMES = {
  light: {
    color: ['#F26B1D', '#2F7FB8', '#2E9E6B', '#8A6BD6', '#B8860B', '#D6457A', '#5B6B7C', '#17A2B8'],
    text: '#2B3440', muted: '#647080', line: '#D5DBE2', surface: '#FFFFFF',
    heat: ['#FDE3D2', '#F26B1D', '#8A2F00'],
  },
  dark: {
    color: ['#F26B1D', '#5AA9E6', '#4FC08D', '#B39DF0', '#E0B83B', '#F07AA6', '#8A96A3', '#4FD1E0'],
    text: '#E6EAEE', muted: '#8A96A3', line: '#3A4552', surface: '#1E252E',
    heat: ['#2B3440', '#C8500C', '#FFD2B8'],
  },
};
for (const [name, t] of Object.entries(THEMES)) {
  echarts.registerTheme('duct-' + name, {
    color: t.color,
    backgroundColor: 'transparent',
    textStyle: { color: t.text, fontFamily: 'ui-sans-serif, system-ui, sans-serif' },
    legend: { textStyle: { color: t.text } },
    categoryAxis: axisTheme(t),
    valueAxis: axisTheme(t),
  });
}
function axisTheme(t) {
  return {
    axisLine: { lineStyle: { color: t.line } },
    axisTick: { lineStyle: { color: t.line } },
    axisLabel: { color: t.muted },
    nameTextStyle: { color: t.muted },
    splitLine: { lineStyle: { color: t.line, opacity: 0.6 } },
  };
}

const STYLE = `
:host { display: block; width: 100%; height: 100%; min-height: 0; }
.wrap { position: relative; width: 100%; height: 100%; }
.plot { position: absolute; inset: 0; }
.msg { position: absolute; inset: 0; display: flex; align-items: center; justify-content: center;
  padding: 1rem; text-align: center; color: var(--duct-muted, #647080); font: 14px ui-sans-serif, system-ui, sans-serif; }
.msg.err { color: var(--duct-danger, #b42318); background: var(--duct-danger-bg, #fef0ee); white-space: pre-wrap; }
.kpi { position: absolute; inset: 0; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: .25rem; }
.kpi b { font: 600 2.4rem/1.1 ui-sans-serif, system-ui, sans-serif; color: var(--duct-text, #2b3440); }
.kpi span { font: 13px ui-sans-serif, system-ui, sans-serif; color: var(--duct-muted, #647080); }
.tbl { position: absolute; inset: 0; overflow: auto; }
table { border-collapse: collapse; width: 100%; font: 13px ui-sans-serif, system-ui, sans-serif; color: var(--duct-text, #2b3440); }
th { position: sticky; top: 0; background: var(--duct-surface-2, #eceff3); text-align: left; padding: .35rem .6rem; white-space: nowrap; font-weight: 600; }
td { padding: .3rem .6rem; border-top: 1px solid var(--duct-border, #d5dbe2); white-space: nowrap; }
td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
`;

// Theme follows the page (html[data-theme] from the shell toggle, else the OS).
const media = matchMedia('(prefers-color-scheme: dark)');
const dark = {
  get matches() {
    const t = document.documentElement.getAttribute('data-theme');
    if (t === 'dark') return true;
    if (t === 'light') return false;
    return media.matches;
  },
  listeners: new Set(),
  addEventListener(type, fn) { if (type === 'change') this.listeners.add(fn); },
  removeEventListener(type, fn) { this.listeners.delete(fn); },
};
const syncDark = () => dark.listeners.forEach((fn) => fn());
media.addEventListener('change', syncDark);
new MutationObserver(syncDark).observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });

function numFmt(o) {
  const loc = 'es-CL';
  let f;
  switch (o.format) {
    case 'integer': f = new Intl.NumberFormat(loc, { maximumFractionDigits: 0 }); break;
    case 'decimal': f = new Intl.NumberFormat(loc, { minimumFractionDigits: 2, maximumFractionDigits: 2 }); break;
    case 'percent': f = new Intl.NumberFormat(loc, { style: 'percent', maximumFractionDigits: 1 }); break;
    case 'compact': f = new Intl.NumberFormat(loc, { notation: 'compact', maximumFractionDigits: 1 }); break;
    default: f = new Intl.NumberFormat(loc, { maximumFractionDigits: 2 });
  }
  const pre = o.prefix || '', suf = o.suffix || '';
  return (v) => (typeof v === 'number' && isFinite(v) ? pre + f.format(v) + suf : v == null ? '' : String(v));
}

class DuctChart extends HTMLElement {
  static get observedAttributes() { return ['data-spec']; }

  constructor() {
    super();
    this.root = this.attachShadow({ mode: 'open' });
    this.root.innerHTML = `<style>${STYLE}</style><div class="wrap"></div>`;
    this.wrap = this.root.querySelector('.wrap');
    this.chart = null;
    this.onTheme = () => this.render();
    this.ro = new ResizeObserver(() => this.chart && this.chart.resize());
  }

  connectedCallback() {
    dark.addEventListener('change', this.onTheme);
    this.ro.observe(this);
    this.render();
  }

  disconnectedCallback() {
    dark.removeEventListener('change', this.onTheme);
    this.ro.disconnect();
    this.dispose();
  }

  attributeChangedCallback() { if (this.isConnected) this.render(); }

  dispose() {
    if (this.chart) { this.chart.dispose(); this.chart = null; }
  }

  message(text, err) {
    this.dispose();
    this.wrap.innerHTML = '';
    const d = document.createElement('div');
    d.className = 'msg' + (err ? ' err' : '');
    if (err) d.setAttribute('role', 'alert');
    d.textContent = text;
    this.wrap.append(d);
  }

  emit(values) {
    this.dispatchEvent(new CustomEvent('ductcross', {
      bubbles: true, composed: true,
      detail: { chart: this.getAttribute('data-chart') || '', values },
    }));
  }

  render() {
    const raw = this.getAttribute('data-spec');
    if (!raw) return;
    let spec;
    try { spec = JSON.parse(raw); } catch { return this.message('Respuesta inválida', true); }
    this.dataset.loaded = '1'; // hides the panel skeleton (ui/input.css)
    if (spec.error) return this.message(spec.error, true);
    if (!spec.rows || spec.rows.length === 0) return this.message('Sin datos', false);
    const o = spec.options || {};
    if (spec.type === 'kpi') return this.renderKPI(spec, o);
    if (spec.type === 'table') return this.renderTable(spec, o);

    const theme = dark.matches ? 'dark' : 'light';
    this.dispose();
    this.wrap.innerHTML = '<div class="plot"></div>';
    const el = this.wrap.firstChild;
    const chart = echarts.init(el, 'duct-' + theme, { renderer: 'canvas' });
    this.chart = chart;
    const built = build(spec, o, THEMES[theme]);
    chart.setOption(built.option);

    if (spec.emit) {
      chart.on('click', (p) => {
        const v = built.pick(p);
        if (v !== undefined) this.emit([v]);
      });
      if (built.brushCats) {
        chart.dispatchAction({
          type: 'takeGlobalCursor', key: 'brush',
          brushOption: { brushType: 'lineX', brushMode: 'single' },
        });
        let clearing = false;
        chart.on('brushEnd', (p) => {
          if (clearing) return;
          const a = p.areas && p.areas[0];
          if (!a || !a.coordRange) return;
          const n = built.brushCats.length;
          const lo = Math.max(0, Math.round(Math.min(...a.coordRange)));
          const hi = Math.min(n - 1, Math.round(Math.max(...a.coordRange)));
          clearing = true;
          chart.dispatchAction({ type: 'brush', areas: [] });
          clearing = false;
          if (hi >= lo) this.emit(built.brushCats.slice(lo, hi + 1));
        });
      }
    }
  }

  renderKPI(spec, o) {
    this.dispose();
    const f = numFmt(o);
    const v = spec.rows[0][0];
    this.wrap.innerHTML = '<div class="kpi"><b></b><span></span></div>';
    this.wrap.querySelector('b').textContent = f(v);
    this.wrap.querySelector('span').textContent = (spec.columns[0] && spec.columns[0].name) || '';
  }

  renderTable(spec, o) {
    this.dispose();
    const f = numFmt(o);
    const box = document.createElement('div');
    box.className = 'tbl';
    const t = document.createElement('table');
    const head = t.createTHead().insertRow();
    spec.columns.forEach((c) => {
      const th = document.createElement('th');
      th.scope = 'col';
      th.textContent = c.name;
      if (c.kind === 'measure') th.className = 'n';
      head.append(th);
    });
    const body = t.createTBody();
    for (const r of spec.rows) {
      const tr = body.insertRow();
      r.forEach((v, i) => {
        const td = tr.insertCell();
        const m = spec.columns[i].kind === 'measure';
        td.textContent = m ? f(v) : v == null ? '' : String(v);
        if (m) td.className = 'n';
      });
    }
    box.append(t);
    this.wrap.innerHTML = '';
    this.wrap.append(box);
  }
}

// uniq keeps first-seen order.
function uniq(xs) { return [...new Set(xs)]; }

// build maps a spec to an ECharts option. pick(params) returns the cross-filter
// value (the first dimension's value) of a clicked element.
function build(spec, o, t) {
  const fmt = numFmt(o);
  const sel = new Set((spec.selected || []).map(String));
  const hasSel = sel.size > 0;
  const base = {
    animationDuration: 300,
    tooltip: { trigger: spec.type === 'pie' || spec.type === 'scatter' || spec.type === 'heatmap' ? 'item' : 'axis', valueFormatter: fmt,
      backgroundColor: t.surface, borderColor: t.line, textStyle: { color: t.text } },
    color: o.colors && o.colors.length ? o.colors : undefined,
  };
  const legendOn = (n) => (o.legend === undefined || o.legend === null ? n > 1 : o.legend);
  const cols = spec.columns;
  const rows = spec.rows;

  switch (spec.type) {
    case 'pie': {
      const data = rows.map((r) => ({ name: String(r[0]), value: r[1],
        itemStyle: hasSel && !sel.has(String(r[0])) ? { opacity: 0.35 } : undefined }));
      return {
        option: { ...base,
          legend: legendOn(2) ? { type: 'scroll', bottom: 0 } : undefined,
          series: [{ type: 'pie', name: cols[1].name, radius: o.donut ? ['45%', '70%'] : '70%',
            center: ['50%', legendOn(2) ? '45%' : '50%'], data,
            label: { show: o.labels !== false, color: t.text, formatter: '{b}' },
            itemStyle: { borderColor: t.surface, borderWidth: 2 } }] },
        pick: (p) => p.name,
      };
    }
    case 'scatter': {
      const label = cols.length > 2 && cols[0].kind === 'dimension';
      const o0 = label ? 1 : 0;
      const sizes = cols.length > o0 + 2 ? rows.map((r) => r[o0 + 2]) : null;
      const max = sizes ? Math.max(...sizes.map(Number), 1) : 1;
      const data = rows.map((r) => ({ name: label ? String(r[0]) : '', value: [r[o0], r[o0 + 1]],
        symbolSize: sizes ? 8 + 28 * Math.sqrt(Math.max(Number(r[o0 + 2]), 0) / max) : 10 }));
      return {
        option: { ...base,
          grid: { left: 56, right: 24, top: 24, bottom: 44, containLabel: true },
          xAxis: { type: o.log ? 'log' : 'value', name: o.x_title || cols[o0].name, nameLocation: 'middle', nameGap: 28, axisLabel: { formatter: fmt } },
          yAxis: { type: 'value', name: o.y_title || cols[o0 + 1].name, axisLabel: { formatter: fmt }, min: o.y_min, max: o.y_max },
          tooltip: { ...base.tooltip, formatter: (p) => `${p.name ? p.name + '<br/>' : ''}${cols[o0].name}: ${fmt(p.value[0])}<br/>${cols[o0 + 1].name}: ${fmt(p.value[1])}` },
          series: [{ type: 'scatter', data }] },
        pick: () => undefined,
      };
    }
    case 'heatmap': {
      const xs = uniq(rows.map((r) => String(r[0])));
      const ys = uniq(rows.map((r) => String(r[1])));
      const vals = rows.map((r) => r[2]).filter((v) => typeof v === 'number');
      const data = rows.map((r) => [xs.indexOf(String(r[0])), ys.indexOf(String(r[1])), r[2]]);
      return {
        option: { ...base,
          grid: { left: 56, right: 24, top: 16, bottom: 72, containLabel: true },
          xAxis: { type: 'category', data: xs, splitArea: { show: true }, name: o.x_title, axisLabel: { hideOverlap: true } },
          yAxis: { type: 'category', data: ys, splitArea: { show: true }, name: o.y_title },
          visualMap: { type: 'continuous', min: Math.min(...vals, 0), max: Math.max(...vals, 1), calculable: false,
            orient: 'horizontal', left: 'center', bottom: 4, inRange: { color: t.heat }, textStyle: { color: t.muted }, formatter: (v) => fmt(v) },
          tooltip: { ...base.tooltip, formatter: (p) => `${xs[p.value[0]]} / ${ys[p.value[1]]}<br/>${cols[2].name}: ${fmt(p.value[2])}` },
          series: [{ type: 'heatmap', data, label: { show: o.labels === true, formatter: (p) => fmt(p.value[2]), color: t.text },
            emphasis: { itemStyle: { borderColor: t.text, borderWidth: 1 } } }] },
        pick: (p) => xs[p.value[0]],
      };
    }
  }

  // line, area, bar: dim 0 = x; optional dim 1 = series (one measure), else one series per measure.
  const nd = cols.filter((c) => c.kind === 'dimension').length;
  let cats, series;
  if (nd === 2) {
    cats = uniq(rows.map((r) => String(r[0])));
    const names = uniq(rows.map((r) => String(r[1])));
    series = names.map((n) => {
      const data = cats.map(() => null);
      for (const r of rows) if (String(r[1]) === n) data[cats.indexOf(String(r[0]))] = r[2];
      return { name: n, data };
    });
  } else {
    cats = rows.map((r) => String(r[0]));
    series = cols.slice(1).map((c, i) => ({ name: c.name, data: rows.map((r) => r[1 + i]) }));
  }
  const bar = spec.type === 'bar';
  const horiz = bar && o.horizontal;
  const out = series.map((s) => {
    const data = hasSel && series.length === 1
      ? s.data.map((v, i) => (bar
        ? { value: v, itemStyle: { opacity: sel.has(cats[i]) ? 1 : 0.35 } }
        : { value: v, symbolSize: sel.has(cats[i]) ? 10 : 5, itemStyle: { opacity: sel.has(cats[i]) ? 1 : 0.4 } }))
      : s.data;
    const r = { type: bar ? 'bar' : 'line', name: s.name, data,
      stack: o.stack ? 'total' : undefined,
      label: o.labels ? { show: true, position: bar ? (horiz ? 'right' : 'top') : 'top', formatter: (p) => fmt(p.value && p.value.value !== undefined ? p.value.value : p.value), color: t.text } : undefined };
    if (!bar) {
      r.smooth = !!o.smooth;
      r.showSymbol = series.length * cats.length <= 120 || hasSel;
      if (spec.type === 'area') r.areaStyle = { opacity: 0.25 };
    } else {
      r.itemStyle = { borderRadius: horiz ? [0, 3, 3, 0] : [3, 3, 0, 0] };
    }
    return r;
  });
  const catAxis = { type: 'category', data: cats, name: horiz ? o.y_title : o.x_title, nameLocation: 'middle', nameGap: 30,
    axisLabel: { hideOverlap: true }, boundaryGap: bar || undefined, inverse: horiz };
  const valAxis = { type: o.log ? 'log' : 'value', name: horiz ? o.x_title : o.y_title, min: o.y_min, max: o.y_max,
    axisLabel: { formatter: fmt } };
  const showLegend = legendOn(series.length);
  const brushable = !horiz && spec.emit;
  return {
    option: { ...base,
      grid: { left: 16, right: 24, top: showLegend ? 36 : 20, bottom: o.x_title && !horiz ? 40 : 16, containLabel: true },
      legend: showLegend ? { top: 0, type: 'scroll' } : undefined,
      xAxis: horiz ? valAxis : catAxis,
      yAxis: horiz ? catAxis : valAxis,
      series: out,
      toolbox: brushable ? { show: false, feature: { brush: { type: ['lineX'] } } } : undefined,
      brush: brushable ? { xAxisIndex: 0, brushType: 'lineX', brushMode: 'single', transformable: false,
        brushStyle: { color: 'rgba(242,107,29,0.15)', borderColor: '#F26B1D' } } : undefined },
    pick: (p) => cats[p.dataIndex],
    brushCats: brushable ? cats : null,
  };
}

customElements.define('duct-chart', DuctChart);
