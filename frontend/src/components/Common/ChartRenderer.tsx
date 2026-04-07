import Plotly from 'plotly.js-dist-min';
import _createPlotlyComponent from 'react-plotly.js/factory';
import { useState } from 'react';

const createPlotlyComponent = (typeof _createPlotlyComponent === 'function') 
    ? _createPlotlyComponent 
    : (_createPlotlyComponent as any)?.default;

let Plot: any = () => <div className="p-4 bg-red-900/10 text-red-400 text-xs rounded-xl">Visualization Engine Offline</div>;

if (typeof createPlotlyComponent === 'function') {
    try {
        Plot = createPlotlyComponent(Plotly);
    } catch (e) {
        console.error("Failed to initialize Plotly component", e);
    }
}

const DESIGN_TOKENS = {
    background: '#ffffff',
    onBackground: '#17325f',
    primary: '#2f5597',
    primaryContainer: '#2f5597',
    secondary: '#2a9d8f',
    secondaryContainer: '#ba3f6c',
    tertiary: '#f2c14e',
    grid: 'rgba(148, 163, 184, 0.25)'
};
const COLOR_PALETTE = ['#2f5597', '#2a9d8f', '#ba3f6c', '#f2c14e', '#6a4c93', '#00a8e8', '#3a86ff', '#8338ec'];

const DEFAULT_LAYOUT = {
    plot_bgcolor: '#ffffff',
    paper_bgcolor: '#ffffff',
    font: {
        family: 'Inter, sans-serif',
        color: DESIGN_TOKENS.onBackground,
        size: 12
    },
    autosize: true,
    margin: { l: 70, r: 24, t: 92, b: 72 },
    hovermode: "closest",
    dragmode: false,
    title: {
        font: { size: 17, color: DESIGN_TOKENS.onBackground },
        x: 0.5,
        xanchor: "center",
        y: 0.98,
        yanchor: "top",
        pad: { t: 8, b: 10 },
    },
    showlegend: false,
    xaxis: {
        gridcolor: DESIGN_TOKENS.grid,
        zerolinecolor: DESIGN_TOKENS.grid,
        showspikes: false,
        tickfont: { color: '#334155', size: 11 },
        title: { standoff: 18, font: { size: 12, color: '#334155' } }
    },
    yaxis: {
        gridcolor: DESIGN_TOKENS.grid,
        zerolinecolor: DESIGN_TOKENS.grid,
        showspikes: false,
        tickfont: { color: '#334155', size: 11 },
        title: { standoff: 16, font: { size: 12, color: '#334155' } }
    },
    bargap: 0.28
};

const _isDateLike = (v: unknown): boolean => {
    if (typeof v !== "string") return false;
    const t = Date.parse(v);
    return !Number.isNaN(t);
};

const _aggregateSeries = (x: unknown[], y: unknown[]) => {
    const rows = x.map((vx, i) => ({ x: String(vx), y: Number(y[i]) })).filter((r) => Number.isFinite(r.y));
    if (!rows.length) return { x: [] as string[], y: [] as number[] };

    if (_isDateLike(rows[0].x)) {
        const bucket = new Map<string, { sum: number; count: number }>();
        for (const r of rows) {
            const d = new Date(r.x);
            if (Number.isNaN(d.getTime())) continue;
            const key = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`;
            const prev = bucket.get(key) || { sum: 0, count: 0 };
            prev.sum += r.y;
            prev.count += 1;
            bucket.set(key, prev);
        }
        const out = [...bucket.entries()].sort((a, b) => a[0].localeCompare(b[0]));
        return { x: out.map(([k]) => k), y: out.map(([, v]) => v.sum / Math.max(v.count, 1)) };
    }

    const step = Math.max(1, Math.ceil(rows.length / 80));
    const out = rows.filter((_r, i) => i % step === 0);
    return { x: out.map((r) => r.x), y: out.map((r) => r.y) };
};

const _canonicalCategory = (v: unknown): string => {
    const raw = String(v ?? "").trim();
    const cleaned = raw.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
    if (!cleaned) return raw;
    return cleaned
        .split(" ")
        .map((p) => p.charAt(0).toUpperCase() + p.slice(1))
        .join(" ");
};

const _compactBarTrace = (trace: any) => {
    const isHorizontal = trace?.orientation === "h";
    const categories = isHorizontal ? (Array.isArray(trace?.y) ? trace.y : []) : (Array.isArray(trace?.x) ? trace.x : []);
    const values = isHorizontal ? (Array.isArray(trace?.x) ? trace.x : []) : (Array.isArray(trace?.y) ? trace.y : []);
    if (!categories.length || categories.length !== values.length) return trace;

    const merged = new Map<string, number>();
    categories.forEach((c: unknown, idx: number) => {
        const key = _canonicalCategory(c);
        const prev = merged.get(key) || 0;
        const v = Number(values[idx]);
        merged.set(key, prev + (Number.isFinite(v) ? v : 0));
    });

    const top = [...merged.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 12);
    const labels = top.map(([k]) => k);
    const nums = top.map(([, v]) => v);
    const barColors = nums.map((_, i) => COLOR_PALETTE[i % COLOR_PALETTE.length]);

    if (isHorizontal) {
        return {
            ...trace,
            x: nums,
            y: labels,
            marker: {
                ...(trace.marker || {}),
                color: barColors,
                line: { color: "rgba(0,0,0,0.15)", width: 1.1 },
            },
            hovertemplate: "%{y}: %{x:,.2f}<extra></extra>",
        };
    }
    return {
        ...trace,
        x: labels,
        y: nums,
        marker: {
            ...(trace.marker || {}),
            color: barColors,
            line: { color: "rgba(0,0,0,0.15)", width: 1.1 },
        },
        hovertemplate: "%{x}: %{y:,.2f}<extra></extra>",
    };
};

const CHART_TYPE_OPTIONS = [
    { value: 'auto', label: 'Auto' },
    { value: 'bar', label: 'Bar' },
    { value: 'stacked_bar', label: 'Stacked Bar' },
    { value: 'line', label: 'Line' },
    { value: 'area', label: 'Area' },
    { value: 'stacked_area', label: 'Stacked Area' },
    { value: 'scatter', label: 'Scatter' },
    { value: 'bubble', label: 'Bubble' },
    { value: 'pie', label: 'Pie' },
    { value: 'donut', label: 'Donut' },
    { value: 'histogram', label: 'Histogram' },
    { value: 'box', label: 'Box' },
    { value: 'violin', label: 'Violin' },
    { value: 'waterfall', label: 'Waterfall' },
    { value: 'funnel', label: 'Funnel' },
    { value: 'radar', label: 'Radar' },
    { value: 'heatmap', label: 'Heatmap' },
    { value: 'treemap', label: 'Treemap' },
    { value: 'sunburst', label: 'Sunburst' },
];

const _applyPreferredType = (trace: any, preferredType: string, idx: number) => {
    if (preferredType === 'auto') return trace;
    const base = {
        ...trace,
        marker: {
            ...(trace.marker || {}),
            color: trace.marker?.color || COLOR_PALETTE[idx % COLOR_PALETTE.length],
        },
        line: {
            ...(trace.line || {}),
            color: trace.line?.color || COLOR_PALETTE[idx % COLOR_PALETTE.length],
            width: trace.line?.width || 3,
        }
    };

    if (preferredType === 'stacked_bar') return { ...base, type: 'bar' };
    if (preferredType === 'stacked_area') return { ...base, type: 'scatter', mode: 'lines', fill: 'tonexty' };
    if (preferredType === 'area') return { ...base, type: 'scatter', mode: 'lines', fill: 'tozeroy' };
    if (preferredType === 'line') return { ...base, type: 'scatter', mode: 'lines+markers', marker: { ...(base.marker || {}), size: 5 } };
    if (preferredType === 'scatter') return { ...base, type: 'scatter', mode: 'markers' };
    if (preferredType === 'bubble') return { ...base, type: 'scatter', mode: 'markers', marker: { ...(base.marker || {}), size: 10, sizemode: 'diameter' } };
    if (preferredType === 'pie') return { ...base, type: 'pie', labels: trace.x, values: trace.y, hole: 0 };
    if (preferredType === 'donut') return { ...base, type: 'pie', labels: trace.x, values: trace.y, hole: 0.55 };
    if (preferredType === 'histogram') return { ...base, type: 'histogram', x: trace.y || trace.x };
    if (preferredType === 'box') return { ...base, type: 'box', y: trace.y, name: trace.name };
    if (preferredType === 'violin') return { ...base, type: 'violin', y: trace.y, name: trace.name, points: false };
    if (preferredType === 'funnel') return { ...base, type: 'funnel', y: trace.x, x: trace.y };
    if (preferredType === 'waterfall') return { ...base, type: 'waterfall', x: trace.x, y: trace.y };
    if (preferredType === 'radar') return {
        ...base,
        type: 'scatterpolar',
        mode: 'lines+markers',
        r: trace.y,
        theta: trace.x,
        fill: 'toself'
    };
    if (preferredType === 'heatmap') return {
        type: 'heatmap',
        z: [(trace.y || []).map((v: unknown) => Number(v))],
        x: trace.x,
        y: [trace.name || 'value'],
        colorscale: 'Blues'
    };
    if (preferredType === 'treemap') return { type: 'treemap', labels: trace.x, parents: (trace.x || []).map(() => ''), values: trace.y };
    if (preferredType === 'sunburst') return { type: 'sunburst', labels: trace.x, parents: (trace.x || []).map(() => ''), values: trace.y };
    if (preferredType === 'bar') return _compactBarTrace({ ...base, type: 'bar' });
    return base;
};

export default function ChartRenderer({ chart, initialPreferredType = 'auto' }: { chart: any; initialPreferredType?: string }) {
    if (!chart) return null;
    const [imageFailed, setImageFailed] = useState(false);
    const [preferredType, setPreferredType] = useState<string>(initialPreferredType || 'auto');

    let data: any[] = [];
    let layout: any = { ...DEFAULT_LAYOUT };
    const categoryCount = Array.isArray(chart?.x) ? chart.x.length : 0;
    const isDenseCategory = categoryCount >= 10;

    if (chart.spec) {
        data = chart.spec.data.map((trace: any, idx: number) => ({
            ...trace,
            marker: {
                ...trace.marker,
                color: trace.marker?.color || COLOR_PALETTE[idx % COLOR_PALETTE.length],
                line: { color: "rgba(0,0,0,0.15)", width: 1 }
            },
            line: {
                ...trace.line,
                color: trace.line?.color || COLOR_PALETTE[idx % COLOR_PALETTE.length],
                width: 3
            }
        }));
        data = data.map((t: any) => {
            const xArr = Array.isArray(t?.x) ? t.x : [];
            const yArr = Array.isArray(t?.y) ? t.y : [];
            if (xArr.length > 120 && yArr.length === xArr.length) {
                const agg = _aggregateSeries(xArr, yArr);
                return {
                    ...t,
                    x: agg.x,
                    y: agg.y,
                    mode: (t.type === "scatter" || t.mode) ? "lines" : t.mode,
                };
            }
            return t;
        });
        data = data.map((t: any) => {
            if (String(t?.type || "").toLowerCase() === "bar") {
                return _compactBarTrace(t);
            }
            if (String(t?.type || "").toLowerCase() === "scatter" && (t?.mode || "").includes("lines")) {
                return {
                    ...t,
                    mode: "lines+markers",
                    marker: { ...(t.marker || {}), size: 4, opacity: 0.75 },
                    line: { ...(t.line || {}), shape: "spline", smoothing: 0.8, width: 3 },
                    fill: t.fill || "tozeroy",
                    fillcolor: "rgba(47,85,151,0.12)",
                };
            }
            return t;
        });
        data = data.map((t: any, idx: number) => _applyPreferredType(t, preferredType, idx));
        layout = { ...DEFAULT_LAYOUT, ...chart.spec.layout };
        layout.showlegend = data.length > 1 || Boolean(chart.spec?.layout?.showlegend);
        if (layout.showlegend) {
            layout.legend = {
                ...(layout.legend || {}),
                orientation: layout.legend?.orientation || "h",
                y: typeof layout.legend?.y === "number" ? layout.legend.y : 1.06,
                x: typeof layout.legend?.x === "number" ? layout.legend.x : 0,
            };
        }
    } else if (chart.image && !imageFailed) {
        const raw = String(chart.image || "");
        const src = raw.startsWith("data:image/")
            ? raw
            : (/^[A-Za-z0-9+/=]+$/.test(raw) ? `data:image/png;base64,${raw}` : raw);
        const isRenderableImage = src.startsWith("data:image/") || src.startsWith("http://") || src.startsWith("https://") || src.startsWith("/static/");
        if (isRenderableImage) {
            return (
                <img
                    src={src}
                    alt="Chart visualization"
                    className="rounded-xl border border-[#cfd8ea] shadow-sm w-full"
                    onError={() => setImageFailed(true)}
                />
            );
        }
    } else {
        const baseTrace = {
            x: chart.x || chart.values,
            y: chart.y,
            marker: { color: DESIGN_TOKENS.primary },
            line: { color: DESIGN_TOKENS.primary, width: 3 }
        };

        switch (chart.type) {
            case "bar":
                if (isDenseCategory) {
                    data = [_compactBarTrace({
                        x: chart.y,
                        y: chart.x,
                        type: "bar",
                        orientation: "h",
                        marker: {
                            color: DESIGN_TOKENS.primaryContainer,
                            line: { color: DESIGN_TOKENS.primary, width: 1.2 }
                        },
                        hovertemplate: "%{y}: %{x:,.2f}<extra></extra>"
                    })];
                } else {
                    data = [_compactBarTrace({
                        ...baseTrace,
                        type: "bar",
                        marker: {
                            ...baseTrace.marker,
                            color: DESIGN_TOKENS.primaryContainer,
                            line: { color: DESIGN_TOKENS.primary, width: 1.2 }
                        },
                        text: Array.isArray(chart.y) ? chart.y.map((v: unknown) => String(v)) : undefined,
                        textposition: "outside",
                        cliponaxis: false
                    })];
                }
                break;
            case "line":
                {
                const xArr = Array.isArray(baseTrace.x) ? baseTrace.x : [];
                const yArr = Array.isArray(baseTrace.y) ? baseTrace.y : [];
                const agg = (xArr.length > 120 && yArr.length === xArr.length)
                    ? _aggregateSeries(xArr, yArr)
                    : { x: xArr, y: yArr };
                data = [{
                    ...baseTrace,
                    x: agg.x,
                    y: agg.y,
                    type: "scatter",
                    mode: "lines+markers",
                    marker: { ...baseTrace.marker, size: 4, color: DESIGN_TOKENS.primaryContainer, opacity: 0.8 },
                    line: { ...baseTrace.line, color: DESIGN_TOKENS.primary, width: 3, shape: "spline", smoothing: 1.0 },
                    fill: "tozeroy",
                    fillcolor: "rgba(47,85,151,0.12)"
                }];
                }
                break;
            case "histogram":
                data = [{
                    ...baseTrace,
                    type: "histogram",
                    marker: { color: DESIGN_TOKENS.secondaryContainer, line: { color: DESIGN_TOKENS.secondary, width: 1 } },
                    opacity: 0.9
                }];
                break;
            case "compare":
                data = [{
                    ...baseTrace,
                    type: "bar",
                    marker: { color: COLOR_PALETTE[0] }
                }];
                break;
            default:
                return <div className="text-[10px] text-[#ffb4ab]">Oracle could not visualize this segment</div>;
        }
        data = data.map((t: any, idx: number) => _applyPreferredType(t, preferredType, idx));
    }

    const isPolarLike = preferredType === 'radar';
    const isHeatLike = preferredType === 'heatmap';

    layout = {
        ...layout,
        title: { ...(layout.title || {}), text: chart?.title || layout?.title?.text || "" },
        xaxis: {
            ...DEFAULT_LAYOUT.xaxis,
            ...(layout.xaxis || {}),
            automargin: true,
            tickangle: (Array.isArray(chart?.x) && chart.x.length > 8) ? -25 : 0,
            nticks: isDenseCategory ? 8 : undefined
        },
        yaxis: {
            ...DEFAULT_LAYOUT.yaxis,
            ...(layout.yaxis || {}),
            automargin: true,
            nticks: isDenseCategory ? 10 : undefined
        },
        polar: isPolarLike ? {
            radialaxis: {
                visible: true,
                gridcolor: DESIGN_TOKENS.grid,
                tickfont: { color: '#334155', size: 11 }
            },
            angularaxis: {
                tickfont: { color: '#334155', size: 11 }
            }
        } : undefined,
        height: Math.max(520, Math.min(920, 420 + (isDenseCategory ? categoryCount * 16 : 48)))
    };
    if (preferredType === 'stacked_bar') {
        layout = { ...layout, barmode: 'stack' };
    }
    if (preferredType === 'stacked_area') {
        layout = { ...layout, hovermode: 'x unified' };
    }
    if (isHeatLike) {
        layout = { ...layout, xaxis: { ...(layout.xaxis || {}), tickangle: -25 }, yaxis: { ...(layout.yaxis || {}), automargin: true } };
    }

    if (!data.length) {
        return (
            <div className="min-h-[240px] flex items-center justify-center rounded-xl border border-[#cfd8ea] text-slate-500 text-sm">
                Chart unavailable for this result. See the data table below.
            </div>
        );
    }

    return (
        <div className="w-full h-full min-h-[520px] bg-white rounded-xl border border-[#cfd8ea] p-3">
            <div className="flex justify-end mb-2">
                <select
                    value={preferredType}
                    onChange={(e) => setPreferredType(e.target.value)}
                    className="h-8 rounded-md border border-[#cfd8ea] bg-[#f8fbff] text-[#17325f] px-2 text-xs"
                >
                    {CHART_TYPE_OPTIONS.map((opt) => (
                        <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                </select>
            </div>
            <Plot
                data={data}
                layout={layout}
                useResizeHandler
                style={{ width: '100%', height: '100%' }}
                config={{
                    displayModeBar: true,
                    displaylogo: false,
                    responsive: true,
                    scrollZoom: false,
                    doubleClick: false,
                    modeBarButtonsToRemove: [
                        "zoom2d",
                        "select2d",
                        "lasso2d",
                        "zoomIn2d",
                        "zoomOut2d",
                        "autoScale2d",
                        "toggleSpikelines",
                    ],
                }}
            />
        </div>
    );
}
