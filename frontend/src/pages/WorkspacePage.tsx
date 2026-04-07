import React from "react";
import { useSessionStore } from "../store/useSessionStore";
import { useQueryStore } from "../store/useQueryStore";
import { useNavigate } from "react-router-dom";
import ResultRenderer from "../components/Common/ResultRenderer";
import QueryComposer from "../components/Chat/QueryComposer";
import { runQuery } from "../services/queryService";
import Layout from "../components/Common/Layout";
import GlassCard from "../components/Common/GlassCard";
import KPIStatsPower from "../components/Dashboard/KPIStatsPower";

const toText = (value: unknown): string => {
    if (typeof value === "string") return value;
    if (typeof value === "number" || typeof value === "boolean") return String(value);
    if (value && typeof value === "object") {
        const obj = value as Record<string, unknown>;
        const preferred = obj.summary ?? obj.text ?? obj.message ?? obj.title;
        if (typeof preferred === "string") return preferred;
    }
    return "";
};

const normalizeQueryLabel = (query: string): string => {
    return query
        .replace(/_/g, " ")
        .replace(/\s+/g, " ")
        .trim();
};

const buildFallbackSuggestions = (datasetSummary: any): string[] => {
    const columns = Array.isArray(datasetSummary?.columns) ? datasetSummary.columns : [];
    const kpis = columns
        .filter((c: any) => c?.semantic_type === "kpi")
        .map((c: any) => c?.name)
        .filter(Boolean);
    const dims = columns
        .filter((c: any) => c?.semantic_type === "dimension")
        .map((c: any) => c?.name)
        .filter(Boolean);
    const times = columns
        .filter((c: any) => c?.semantic_type === "date" || c?.semantic_type === "time")
        .map((c: any) => c?.name)
        .filter(Boolean);

    const suggestions: string[] = [];
    const kpi = kpis[0];
    if (kpi) suggestions.push(`show ${kpi}`);
    if (kpi && dims[0]) suggestions.push(`show ${kpi} by ${dims[0]}`);
    if (kpi && times[0]) suggestions.push(`show ${kpi} over time`);
    if (kpis.length > 1) suggestions.push(`compare ${kpis[0]} with ${kpis[1]}`);
    if (dims[0]) suggestions.push(`list top 5 ${dims[0]} by ${kpi || "value"}`);

    return [...new Set(suggestions)].slice(0, 8);
};

const toDisplayLabel = (v: unknown): string => {
    return String(v ?? "")
        .replace(/_/g, " ")
        .replace(/\s+/g, " ")
        .trim()
        .replace(/\b\w/g, (m) => m.toUpperCase());
};

const insightTone = (text: string): { chip: string; border: string; bg: string; label: string } => {
    const t = text.toLowerCase();
    if (t.includes("decrease") || t.includes("drop") || t.includes("lowest")) {
        return {
            chip: "text-rose-700 bg-rose-100 border-rose-200",
            border: "border-rose-200",
            bg: "bg-rose-50/55",
            label: "Risk",
        };
    }
    if (t.includes("increase") || t.includes("highest") || t.includes("top") || t.includes("contributes")) {
        return {
            chip: "text-emerald-700 bg-emerald-100 border-emerald-200",
            border: "border-emerald-200",
            bg: "bg-emerald-50/55",
            label: "Opportunity",
        };
    }
    if (t.includes("spike") || t.includes("anomaly")) {
        return {
            chip: "text-amber-700 bg-amber-100 border-amber-200",
            border: "border-amber-200",
            bg: "bg-amber-50/55",
            label: "Anomaly",
        };
    }
    return {
        chip: "text-blue-700 bg-blue-100 border-blue-200",
        border: "border-blue-200",
        bg: "bg-blue-50/55",
        label: "Insight",
    };
};

const mapDashboardKpisToCards = (kpis: any[]): Array<{ label: string; value: string | number; description?: string }> => {
    if (!Array.isArray(kpis)) return [];
    return kpis
        .slice(0, 4)
        .map((k) => {
            const label = toDisplayLabel(k?.name || k?.label || "Metric");
            const raw = k?.total ?? k?.value ?? k?.average ?? k?.max ?? k?.min ?? "-";
            const value = typeof raw === "number" ? raw.toLocaleString(undefined, { maximumFractionDigits: 2 }) : String(raw);
            const description = typeof k?.average === "number"
                ? `avg ${k.average.toLocaleString(undefined, { maximumFractionDigits: 2 })}`
                : undefined;
            return { label, value, description };
        })
        .filter((x) => x.label !== "Metric" || x.value !== "-");
};

const WorkspacePage: React.FC = () => {
    const navigate = useNavigate();
    const reset = useSessionStore((s) => s.clearSession);
    const session = useSessionStore((s) => s);
    const setSuggestions = useSessionStore((s) => s.setSuggestions);
    const lastResult = useQueryStore((s) => s.lastResult);

    const setInput = useQueryStore((s) => s.setInput);
    const setLoading = useQueryStore((s) => s.setLoading);
    const addToHistory = useQueryStore((s) => s.addToHistory);

    const handleSuggestionClick = async (suggestion: string) => {
        if (!session.id) return;
        setInput(suggestion);
        setLoading(true);
        try {
            const res = await runQuery(session.id, suggestion);
            addToHistory({ query: suggestion, response: res, status: res.status });
            if (Array.isArray(res?.suggestions?.items)) {
                setSuggestions(res.suggestions.items);
            }
        } catch (err: any) {
            console.error(err);
        }
        setLoading(false);
    };

    if (!session.id) {
        return (
            <Layout>
                <div className="flex flex-col items-center justify-center min-h-[60vh] space-y-6">
                    <div className="w-16 h-16 bg-[#414751]/10 rounded-2xl flex items-center justify-center border border-[#414751]/10">
                        <svg className="w-8 h-8 text-[#a0caff]/20" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.268 16c-.77 1.333.192 3 1.732 3z" /></svg>
                    </div>
                    <div className="text-center space-y-2">
                        <h2 className="text-xl font-bold opacity-80">Oracle Offline</h2>
                        <p className="text-[#c1c7d2]/60 text-sm">Synchronize a dataset to initiate insight flows.</p>
                    </div>
                    <button 
                        onClick={() => navigate('/')}
                        className="px-8 py-3 bg-[#a0caff] text-[#003259] font-bold rounded-full hover:scale-105 transition-all text-sm uppercase tracking-widest shadow-xl"
                    >
                        Sync Dataset
                    </button>
                </div>
            </Layout>
        );
    }

    const mode = session.mode || "both";
    const isDashboardMode = mode === "dashboard";
    const isQueryMode = mode === "query";

    const mainChart = session.dashboard?.charts?.[0];
    const secondaryCharts = session.dashboard?.charts?.slice(1) || [];
    const supportingInsights = (session.dashboard?.insights || [])
        .map((i: unknown) => toText(i))
        .filter((i: string) => i.length > 0)
        .slice(0, 5);
    const computedFallbackSuggestions = buildFallbackSuggestions(session.datasetSummary);
    const mainSuggestions = (session.suggestions?.length ? session.suggestions : computedFallbackSuggestions).slice(0, 8);
    const kpis = session.dashboard?.kpis || [];
    const kpiCards = mapDashboardKpisToCards(kpis);

    return (
        <Layout>
            <div className="space-y-6 px-2">
                <header className="rounded-xl bg-[#1e3a72] text-white px-5 py-4 border border-[#2e4f8d] shadow-sm">
                    <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
                        <div>
                            <h1 className="text-3xl font-bold tracking-tight">Dataset Analysis Dashboard</h1>
                            <p className="text-sm text-blue-100/90">PowerBI-style insight surface for business users</p>
                        </div>
                        <div className="grid grid-cols-2 md:grid-cols-3 gap-2 min-w-[360px]">
                            <div className="bg-[#284b87] border border-[#3c62a5] rounded px-3 py-2">
                                <p className="text-[10px] uppercase tracking-widest text-blue-100">Mode</p>
                                <p className="text-sm font-semibold">{mode.toUpperCase()}</p>
                            </div>
                            <div className="bg-[#284b87] border border-[#3c62a5] rounded px-3 py-2">
                                <p className="text-[10px] uppercase tracking-widest text-blue-100">Rows</p>
                                <p className="text-sm font-semibold">{session.datasetSummary?.row_count ?? "-"}</p>
                            </div>
                            <div className="bg-[#284b87] border border-[#3c62a5] rounded px-3 py-2">
                                <p className="text-[10px] uppercase tracking-widest text-blue-100">Columns</p>
                                <p className="text-sm font-semibold">{session.datasetSummary?.column_count ?? "-"}</p>
                            </div>
                        </div>
                    </div>
                </header>

                <header className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 border-b border-slate-700/30 pb-4">
                    <div className="flex flex-wrap items-center gap-3">
                        <span className="inline-flex items-center gap-2 px-3 py-1 rounded-full bg-[#16335f] border border-[#315087] text-[10px] uppercase tracking-widest text-white">
                            <span className="w-2 h-2 rounded-full bg-blue-500" />
                            Session Active
                        </span>
                        <span className="text-[10px] font-mono text-slate-300">{session.id}</span>
                        <span className="text-[10px] uppercase tracking-widest text-[#4f94dd]">mode: {mode}</span>
                    </div>
                    <button
                        onClick={() => { reset(); navigate("/"); }}
                        className="text-[10px] font-bold uppercase tracking-widest text-red-500 hover:text-red-400 transition-colors"
                    >
                        Terminate Session
                    </button>
                </header>

                <div className="grid grid-cols-1 xl:grid-cols-12 gap-5">
                    <section className="xl:col-span-9 space-y-7">
                        {!isQueryMode && (
                            <>
                                <GlassCard className="!p-8 !bg-white !border-[#cfd8ea] shadow-sm">
                                    <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f] mb-3">Primary insight</p>
                                    <h1 className="text-3xl font-bold tracking-tight text-[#17325f] leading-tight">
                                        {toText(session.dashboard?.primary_insight) || "Dataset ready for exploration"}
                                    </h1>
                                </GlassCard>

                                <KPIStatsPower
                                    metrics={kpiCards.length > 0 ? kpiCards : [
                                        { label: "Data Points", value: supportingInsights.length || 0 },
                                        { label: "Visualizations", value: (session.dashboard?.charts?.length) || 0 },
                                        { label: "Status", value: "Ready" },
                                        { label: "Confidence", value: "High" }
                                    ]}
                                />
                            </>
                        )}

                        {!isDashboardMode && (
                            <GlassCard className="!p-5 !bg-white !border-[#cfd8ea] shadow-sm">
                                <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f] mb-3">Ask dataset</p>
                                <QueryComposer borderNone />
                            </GlassCard>
                        )}

                        {lastResult && (
                            <GlassCard className="!p-6 !bg-white !border-[#cfd8ea] shadow-sm">
                                <ResultRenderer result={lastResult} />
                            </GlassCard>
                        )}

                        {!isQueryMode && mainChart && (
                            <GlassCard className="!p-6 !bg-white !border-[#cfd8ea] shadow-sm">
                                <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f] mb-4">Main chart</p>
                                <ResultRenderer result={{ status: "RESOLVED", charts: [mainChart] }} />
                            </GlassCard>
                        )}

                        {!isQueryMode && secondaryCharts.length > 0 && (
                            secondaryCharts.length === 1 ? (
                                <GlassCard className="!p-5 !bg-white !border-[#cfd8ea] shadow-sm">
                                    <ResultRenderer result={{ status: "RESOLVED", charts: [secondaryCharts[0]] }} />
                                </GlassCard>
                            ) : (
                                <div className="grid grid-cols-1 2xl:grid-cols-2 gap-5 items-start">
                                    {secondaryCharts.map((c: any, i: number) => (
                                        <GlassCard
                                            key={i}
                                            className={`!p-5 !bg-white !border-[#cfd8ea] shadow-sm self-start ${
                                                secondaryCharts.length % 2 === 1 && i === secondaryCharts.length - 1
                                                    ? "2xl:col-span-2"
                                                    : ""
                                            }`}
                                        >
                                            <ResultRenderer result={{ status: "RESOLVED", charts: [c] }} />
                                        </GlassCard>
                                    ))}
                                </div>
                            )
                        )}
                    </section>

                    <aside className="xl:col-span-3 space-y-4 xl:sticky xl:top-6 self-start">
                        {isDashboardMode && (
                            <GlassCard className="!p-5 !bg-white !border-[#cfd8ea] shadow-sm">
                                <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f] mb-3">Ask dataset</p>
                                <p className="text-sm text-slate-600">
                                    Querying is disabled for this session because mode is set to dashboard.
                                </p>
                            </GlassCard>
                        )}

                        {supportingInsights.length > 0 && (
                            <GlassCard className="!p-5 !bg-white !border-[#cfd8ea] shadow-sm">
                                <div className="flex items-center justify-between mb-3">
                                    <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f]">Insights</p>
                                    <span className="text-[10px] font-semibold px-2 py-1 rounded-full border border-[#cdd9ef] bg-[#eef4ff] text-[#365a96]">
                                        {supportingInsights.length} items
                                    </span>
                                </div>
                                <ul className="space-y-2.5">
                                    {supportingInsights.map((insight: string, idx: number) => {
                                        const tone = insightTone(insight);
                                        return (
                                            <li
                                                key={idx}
                                                className={`rounded-lg border ${tone.border} ${tone.bg} p-3`}
                                            >
                                                <div className="flex items-start justify-between gap-2 mb-1.5">
                                                    <span className="text-[10px] font-bold uppercase tracking-widest text-slate-500">
                                                        #{idx + 1}
                                                    </span>
                                                    <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full border ${tone.chip}`}>
                                                        {tone.label}
                                                    </span>
                                                </div>
                                                <p className="text-sm text-slate-700 leading-relaxed">{insight}</p>
                                            </li>
                                        );
                                    })}
                                </ul>
                            </GlassCard>
                        )}

                        <GlassCard className="!p-5 !bg-white !border-[#cfd8ea] shadow-sm">
                            <p className="text-xs uppercase tracking-[0.2em] text-[#2f4b7f] mb-3">Suggestions</p>
                            <div className="flex flex-wrap gap-2">
                                {mainSuggestions.length > 0 ? mainSuggestions.map((s: string, i: number) => (
                                    <button
                                        key={i}
                                        onClick={() => handleSuggestionClick(s)}
                                        className="px-3 py-2 bg-[#eff4ff] border border-[#c6d4ee] rounded-lg text-xs text-[#1f3a6b] hover:bg-[#dfe9ff] hover:border-[#9cb5e3] transition-all"
                                    >
                                        {normalizeQueryLabel(s)}
                                    </button>
                                )) : (
                                    <p className="text-sm text-slate-500">No suggestions available for this session yet.</p>
                                )}
                            </div>
                        </GlassCard>
                    </aside>
                </div>
            </div>
        </Layout>
    );
};

export default WorkspacePage;
