import ChartRenderer from "./ChartRenderer";
import DataTable from "./DataTable";
import { useQueryStore } from "../../store/useQueryStore";
import { useSessionStore } from "../../store/useSessionStore";
import { runQuery } from "../../services/queryService";
import TracePanel from "./TracePanel";
import StatusBanner from "./StatusBanner";

const toText = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  if (value && typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const first = obj.summary ?? obj.text ?? obj.message ?? obj.title;
    if (typeof first === "string") return first;
  }
  return "";
};

const normalizeItems = (values: unknown): string[] => {
  if (!Array.isArray(values)) return [];
  return values
    .map(toText)
    .map((v) => v.replace(/â€¢/g, "•").replace(/\s+/g, " ").trim())
    .filter((v) => v.length > 0 && v !== "•");
};

const extractRows = (result: any): Record<string, unknown>[] => {
  const fromTable = Array.isArray(result?.table) ? result.table : [];
  if (fromTable.length && typeof fromTable[0] === "object") {
    return fromTable.slice(0, 25);
  }

  if (Array.isArray(result?.data)) {
    for (const entry of result.data) {
      if (Array.isArray(entry?.data) && entry.data.length && typeof entry.data[0] === "object") {
        return entry.data.slice(0, 25);
      }
    }
  }

  const chart = Array.isArray(result?.charts) ? result.charts[0] : null;
  if (chart && Array.isArray(chart.x) && Array.isArray(chart.y) && chart.x.length === chart.y.length) {
    const rows = chart.x.slice(0, 25).map((x: unknown, idx: number) => ({
      x,
      value: chart.y[idx],
    }));
    return rows;
  }

  return [];
};

const _toNum = (v: unknown): number => {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

const _canonicalCategory = (v: unknown): string => {
  const raw = String(v ?? "").trim();
  const cleaned = raw.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
  if (!cleaned) return raw;
  return cleaned.split(" ").map((p) => p.charAt(0).toUpperCase() + p.slice(1)).join(" ");
};

const _labelizeMetric = (v: unknown, fallback: string): string => {
  const raw = String(v ?? "").trim();
  if (!raw) return fallback;
  const cleaned = raw
    .replace(/^total\s+/i, "")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  return cleaned
    .split(" ")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(" ");
};

const buildChartsFromData = (result: any): any[] => {
  const series = Array.isArray(result?.data) ? result.data : [];
  if (!series.length) return [];

  // Compare case: build grouped bar from first two KPI series on shared dimension key.
  if (series.length >= 2) {
    const s1 = series[0];
    const s2 = series[1];
    const arr1 = Array.isArray(s1?.data) ? s1.data : [];
    const arr2 = Array.isArray(s2?.data) ? s2.data : [];
    if (arr1.length && arr2.length && typeof arr1[0] === "object" && typeof arr2[0] === "object") {
      const dimKey = Object.keys(arr1[0]).find((k) => k !== "value") || "dimension";
      const map1 = new Map<string, number>();
      const map2 = new Map<string, number>();
      for (const r of arr1) {
        const key = _canonicalCategory(r?.[dimKey]);
        map1.set(key, (map1.get(key) || 0) + _toNum(r?.value));
      }
      for (const r of arr2) {
        const key = _canonicalCategory(r?.[dimKey]);
        map2.set(key, (map2.get(key) || 0) + _toNum(r?.value));
      }
      const cats = [...new Set([...map1.keys(), ...map2.keys()])];
      const top = cats
        .map((c) => ({ c, score: (_toNum(map1.get(c)) + _toNum(map2.get(c))) }))
        .sort((a, b) => b.score - a.score)
        .slice(0, 12)
        .map((x) => x.c);

      const max1 = Math.max(...top.map((c) => _toNum(map1.get(c))), 1);
      const max2 = Math.max(...top.map((c) => _toNum(map2.get(c))), 1);
      const y1 = top.map((c) => (_toNum(map1.get(c)) / max1) * 100);
      const y2 = top.map((c) => (_toNum(map2.get(c)) / max2) * 100);

      let metric1 = _labelizeMetric(s1?.kpi, "Metric 1");
      let metric2 = _labelizeMetric(s2?.kpi, "Metric 2");
      if (metric1.toLowerCase() === metric2.toLowerCase()) {
        metric2 = `${metric2} (Series 2)`;
      }

      return [{
        type: "bar",
        title: `${metric1} vs ${metric2} by ${_labelizeMetric(dimKey, "Dimension")} (0-100 Index)`,
        spec: {
          data: [
            {
              type: "bar",
              name: metric1,
              x: top,
              y: y1,
              marker: { color: "#2f5597" },
              hovertemplate: `<b>Department:</b> %{x}<br><b>${metric1} Index:</b> %{y:.2f}<extra></extra>`,
            },
            {
              type: "bar",
              name: metric2,
              x: top,
              y: y2,
              marker: { color: "#ba3f6c" },
              hovertemplate: `<b>Department:</b> %{x}<br><b>${metric2} Index:</b> %{y:.2f}<extra></extra>`,
            }
          ],
          layout: {
            barmode: "group",
            margin: { l: 72, r: 24, t: 110, b: 72 },
            title: { x: 0.5, xanchor: "center", y: 0.98, yanchor: "top" },
            legend: {
              orientation: "h",
              x: 0,
              y: 1.06,
              title: { text: "Metrics" },
            },
            hovermode: "closest",
            xaxis: { title: dimKey, tickangle: -20 },
            yaxis: { title: "Relative Score (0-100)", range: [0, 105] },
            annotations: [
              {
                x: 0,
                y: 1.2,
                xref: "paper",
                yref: "paper",
                showarrow: false,
                align: "left",
                text: "Each department has two bars: one per metric. Values are normalized to a 0-100 index to compare scale differences.",
                font: { size: 11, color: "#475569" },
              },
            ],
          }
        }
      }];
    }
  }

  return series.slice(0, 2).map((s: any) => {
    const arr = Array.isArray(s?.data) ? s.data : [];
    if (!arr.length || typeof arr[0] !== "object") return null;
    const dimKey = Object.keys(arr[0]).find((k) => k !== "value") || "x";
    const isTime = /date|time|month|year/i.test(dimKey);
    const mapped = arr.map((r: any) => ({ x: String(r?.[dimKey]), y: _toNum(r?.value) }));
    let plotRows = mapped;
    let chartType = "bar";

    if (isTime) {
      chartType = "line";
      const parsed = mapped
        .map((r: { x: string; y: number }) => ({ t: new Date(r.x), y: r.y }))
        .filter((r: { t: Date; y: number }) => !Number.isNaN(r.t.getTime()))
        .sort((a: { t: Date; y: number }, b: { t: Date; y: number }) => a.t.getTime() - b.t.getTime());
      if (parsed.length > 120) {
        const bucket = new Map<string, { sum: number; count: number }>();
        for (const p of parsed) {
          const key = `${p.t.getFullYear()}-${String(p.t.getMonth() + 1).padStart(2, "0")}`;
          const prev = bucket.get(key) || { sum: 0, count: 0 };
          prev.sum += p.y;
          prev.count += 1;
          bucket.set(key, prev);
        }
        plotRows = [...bucket.entries()].map(([x, v]: [string, { sum: number; count: number }]) => ({ x, y: v.sum / Math.max(v.count, 1) }));
      } else {
        plotRows = parsed.map((p: { t: Date; y: number }) => ({ x: p.t.toISOString().slice(0, 10), y: p.y }));
      }
    } else {
      plotRows = mapped.sort((a: { x: string; y: number }, b: { x: string; y: number }) => b.y - a.y).slice(0, 24);
    }

    return {
      type: chartType,
      title: s?.kpi || "Metric",
      x: plotRows.map((r: { x: string; y: number }) => r.x),
      y: plotRows.map((r: { x: string; y: number }) => r.y),
    };
  }).filter(Boolean);
};

export default function ResultRenderer({ result }: { result: any }) {
  const setInput = useQueryStore((s) => s.setInput);
  const setLoading = useQueryStore((s) => s.setLoading);
  const addToHistory = useQueryStore((s) => s.addToHistory);
  const sessionId = useSessionStore((s) => s.id);

  const triggerQuery = async (queryText: string) => {
    if (!sessionId) return;
    setLoading(true);
    try {
      const res = await runQuery(sessionId, queryText);
      addToHistory({ query: queryText, response: res, status: res.status });
    } catch (err) {
      console.error(err);
    }
    setLoading(false);
  };

  const handleSuggestion = (suggestion: string) => {
    setInput(suggestion);
    triggerQuery(suggestion);
  };

  if (!result) return null;
  const insightItems = normalizeItems(result.insights);
  const suggestionItems = normalizeItems(result.suggestions?.items);
  const tableRows = extractRows(result);
  const rawCharts = Array.isArray(result?.charts) ? result.charts : [];
  const isRenderableChart = (c: any) =>
    Boolean(
      c &&
      (
        c.spec ||
        c.image ||
        (Array.isArray(c?.x) && c.x.length) ||
        (Array.isArray(c?.y) && c.y.length) ||
        (Array.isArray(c?.values) && c.values.length) ||
        c.type
      )
    );
  const derivedCharts = buildChartsFromData(result);
  const filteredRawCharts = rawCharts.filter(isRenderableChart);
  const filteredDerivedCharts = derivedCharts.filter(isRenderableChart);
  const hasOnlyImageCharts =
    filteredRawCharts.length > 0 && filteredRawCharts.every((c: any) => c?.image && !c?.spec);
  const isCompareIntent = String(result?.intent?.intent || "").toUpperCase() === "COMPARE";
  const chartItems = ((isCompareIntent && filteredDerivedCharts.length > 0) || (hasOnlyImageCharts && filteredDerivedCharts.length > 0))
    ? filteredDerivedCharts
    : (filteredRawCharts.length > 0 ? filteredRawCharts : filteredDerivedCharts);
  const hasManyCharts = (chartItems?.length || 0) > 1;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <StatusBanner status={result.status} />
        {result.trace && <TracePanel trace={result.trace} />}
      </div>

      <div className="space-y-6">
        {result.status === "RESOLVED" && (
            <div className="space-y-6">
                <div className={hasManyCharts ? "grid grid-cols-1 xl:grid-cols-2 gap-5 items-start" : "space-y-4"}>
                  {chartItems?.map((c: any, i: number) => (
                      <div
                        key={i}
                        className={`animate-fade-in ${
                          hasManyCharts && chartItems.length % 2 === 1 && i === chartItems.length - 1
                            ? "xl:col-span-2"
                            : ""
                        }`}
                        style={{ animationDelay: `${i * 100}ms` }}
                      >
                           <ChartRenderer chart={c} />
                      </div>
                  ))}
                </div>
                {insightItems.length > 0 && (
                    <ul className="space-y-4 pt-4 border-t border-[#dbe4f3]">
                        {insightItems.map((insight: string, idx: number) => (
                        <li key={idx} className="flex space-x-3 items-start group">
                            <span className="w-1.5 h-1.5 rounded-full bg-[#2f5597] mt-2 group-hover:scale-150 transition-transform"></span>
                            <span className="text-sm text-[#334155] group-hover:text-[#0f172a] transition-colors">{insight}</span>
                        </li>
                        ))}
                    </ul>
                )}
                {tableRows.length > 0 && (
                  <div className="pt-4 border-t border-[#dbe4f3]">
                    <div className="max-h-[320px] overflow-auto rounded-xl">
                      <DataTable rows={tableRows} />
                    </div>
                  </div>
                )}
            </div>
        )}

        {(result.status === "INCOMPLETE" || result.status === "AMBIGUOUS") && (
            <div className="p-6 bg-[#fff7e8] border border-[#f3ddb1] rounded-2xl space-y-4">
                <div className="flex items-center space-x-3 text-[#9a6d18]">
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                    <h4 className="font-bold uppercase tracking-widest text-[10px]">Follow-up Required</h4>
                </div>
                <h3 className="text-xl font-medium text-[#17325f]">{result.status === "INCOMPLETE" ? "Need more info" : "Which one did you mean?"}</h3>
                <div className="flex flex-wrap gap-2 pt-2">
                    {suggestionItems.map((s: string, i: number) => (
                        <button 
                            key={i} 
                            onClick={() => handleSuggestion(s)}
                            className="px-4 py-2 bg-white hover:bg-[#f8fbff] border border-[#d4dfef] rounded-full text-xs text-[#1f3a6b] transition-all"
                        >
                            {s}
                        </button>
                    ))}
                </div>
            </div>
        )}

        {result.status === "MODE_BLOCKED" && (
            <div className="p-6 bg-[#eef2f9] border border-[#d0d9e8] rounded-2xl flex items-center space-x-4">
                 <div className="w-10 h-10 rounded-full bg-white flex items-center justify-center text-[#9f3658] border border-[#d0d9e8]">
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m0 0v2m0-2h2m-2 0H8m13 0a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                 </div>
                 <div>
                    <h4 className="text-[10px] font-bold uppercase tracking-widest text-[#64748b]">System Limitation</h4>
                    <p className="text-sm text-[#334155]">{result.message}</p>
                 </div>
            </div>
        )}
      </div>
    </div>
  );
}
