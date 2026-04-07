import React from "react";
import { useSessionStore } from "../store/useSessionStore";
import { useQueryStore } from "../store/useQueryStore";
import { useNavigate } from "react-router-dom";
import ResultRenderer from "../components/Common/ResultRenderer";
import QueryComposer from "../components/Chat/QueryComposer";
import { runQuery } from "../services/queryService";
import Layout from "../components/Common/Layout";
import GlassCard from "../components/Common/GlassCard";
import KPIStats from "../components/Dashboard/KPIStats";

const WorkspacePage: React.FC = () => {
    const navigate = useNavigate();
    const reset = useSessionStore((s) => s.clearSession);
    const session = useSessionStore((s) => s);
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

    const mainChart = session.dashboard?.charts?.[0];
    const secondaryCharts = session.dashboard?.charts?.slice(1) || [];
    const supportingInsights = session.dashboard?.insights?.slice(0, 5) || [];
    const mainSuggestions = session.suggestions?.slice(0, 6) || [];
    const kpis = session.dashboard?.kpis || [];

    return (
        <Layout>
            <div className="max-w-5xl mx-auto space-y-8 pb-32 px-4 relative">
                
                {/* [ HEADER ] */}
                <header className="flex justify-between items-center border-b border-slate-800 pb-4 mt-8">
                    <div className="flex items-center space-x-4">
                         <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                         <span className="text-[10px] font-bold uppercase tracking-widest text-slate-400">Context Active</span>
                         <span className="text-[10px] font-mono text-slate-600">{session.id}</span>
                    </div>
                    <button
                        onClick={() => { reset(); navigate("/"); }}
                        className="text-[10px] font-bold uppercase tracking-widest text-red-400 hover:text-red-300 transition-colors"
                    >
                        Terminate Session
                    </button>
                </header>

                {/* PRIMARY INSIGHT */}
                <section className="py-6">
                    <h1 className="text-4xl font-bold tracking-tight text-white leading-tight max-w-4xl">
                        {session.dashboard?.primary_insight}
                    </h1>
                </section>

                <section>
                    <KPIStats 
                        metrics={kpis.length > 0 ? kpis.slice(0, 4) : [
                            { label: 'Data Points', value: supportingInsights.length || 0 },
                            { label: 'Visualizations', value: (session.dashboard?.charts?.length) || 0 },
                            { label: 'Status', value: 'Ready' },
                            { label: 'Confidence', value: 'High' }
                        ]} 
                    />
                </section>

                {mainChart && (
                    <section>
                         <GlassCard className="!p-8">
                             <div className="flex items-center space-x-3 mb-6">
                                 <span className="text-xs font-bold uppercase tracking-widest text-blue-400">Main Analysis</span>
                             </div>
                             <ResultRenderer result={{ status: "RESOLVED", charts: [mainChart] }} />
                         </GlassCard>
                    </section>
                )}

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-12 pt-8">
                    <section className="space-y-6">
                        <div className="flex items-center space-x-4">
                            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-500">Key Evidence</h3>
                        </div>
                        <ul className="space-y-4">
                            {supportingInsights.map((insight: string, idx: number) => (
                                <li key={idx} className="flex items-start space-x-4">
                                    <span className="text-blue-500 font-bold text-sm mt-0.5">0{idx + 1}</span>
                                    <p className="text-slate-300 leading-relaxed">
                                        {insight}
                                    </p>
                                </li>
                            ))}
                        </ul>
                    </section>

                    <section className="space-y-6">
                        <div className="flex items-center space-x-4">
                            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-500">Explorations</h3>
                        </div>
                        <div className="flex gap-2 flex-wrap">
                            {mainSuggestions.map((s: string, i: number) => (
                                <button 
                                    key={i} 
                                    onClick={() => handleSuggestionClick(s)}
                                    className="px-4 py-2 bg-slate-800 border border-slate-700 rounded-lg text-xs text-slate-300 hover:border-blue-500/50 hover:bg-slate-700 hover:text-white transition-all shadow-sm"
                                >
                                    {s}
                                </button>
                            ))}
                        </div>
                    </section>
                </div>

                {secondaryCharts.length > 0 && (
                    <section className="pt-16 space-y-8">
                         <div className="flex items-center space-x-4">
                            <h3 className="text-xs font-bold uppercase tracking-widest text-slate-500">Additional Visuals</h3>
                        </div>
                        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                            {secondaryCharts.map((c: any, i: number) => (
                                <GlassCard key={i} className="!p-6 border-slate-800 bg-slate-900 shadow-xl">
                                     <ResultRenderer result={{ status: "RESOLVED", charts: [c] }} />
                                </GlassCard>
                            ))}
                        </div>
                    </section>
                )}

                {/* COMMAND BAR - fixed-height simpler layout */}
                <div className="fixed bottom-0 left-0 lg:left-64 right-0 bg-[#0f172a]/95 border-t border-slate-800 p-6 z-50 backdrop-blur-md">
                    <div className="max-w-4xl mx-auto flex flex-col space-y-4">
                        {lastResult && (
                             <GlassCard className="!p-6 border-blue-900/50 shadow-2xl bg-slate-900">
                                <ResultRenderer result={lastResult} />
                             </GlassCard>
                        )}
                        <QueryComposer borderNone />
                    </div>
                </div>
            </div>
        </Layout>
    );
};

export default WorkspacePage;
