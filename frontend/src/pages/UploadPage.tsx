import React, { useState, useRef } from "react";
import { uploadCSV } from "../services/uploadService";
import { useSessionStore } from "../store/useSessionStore";
import { useNavigate } from "react-router-dom";
import { logEvent } from "../utils/logger";
import Layout from "../components/Common/Layout";
import GlassCard from "../components/Common/GlassCard";

const UploadPage: React.FC = () => {
    const [file, setFile] = useState<File | null>(null);
    const [mode, setMode] = useState<"dashboard" | "query" | "both">("both");
    const [isDragActive, setIsDragActive] = useState(false);
    const [loading, setLoading] = useState(false);
    const [backendHealthy, setBackendHealthy] = useState<boolean | null>(null);
    const setSession = useSessionStore((s) => s.setSession);
    const navigate = useNavigate();
    const fileInputRef = useRef<HTMLInputElement>(null);

    React.useEffect(() => {
        try {
            const raw = localStorage.getItem("talkingbi_user_preferences_v1");
            if (raw) {
                const parsed = JSON.parse(raw);
                if (parsed?.defaultMode && ["dashboard", "query", "both"].includes(parsed.defaultMode)) {
                    setMode(parsed.defaultMode);
                }
            }
        } catch {
            // ignore malformed local settings
        }
    }, []);

    React.useEffect(() => {
        const checkHealth = async () => {
            try {
                const res = await fetch("http://127.0.0.1:8000/health");
                setBackendHealthy(res.ok);
            } catch {
                setBackendHealthy(false);
            }
        };
        checkHealth();
    }, []);

    const handleFile = (f: File) => {
        if (f.name.endsWith('.csv')) {
            setFile(f);
            logEvent('FILE_SELECTED', { name: f.name });
        } else {
            alert('Please select a CSV file');
        }
    };

    const handleUpload = async (overrideFile?: File) => {
        const fileToUpload = overrideFile || file;
        if (!fileToUpload) return;

        setLoading(true);
        try {
            const res = await uploadCSV(fileToUpload, mode);
            logEvent('UPLOAD_SUCCESS', { datasetId: res.dataset_id });
            setSession(res);
            navigate("/workspace");
        } catch (err: any) {
            logEvent('UPLOAD_ERROR', err);
            console.error(err);
            alert(err.message || "Upload failed. Please check if the backend is running at http://127.0.0.1:8000");
        }
        setLoading(false);
    };

    const handleRecentClick = async (filename: string) => {
        // Create a dummy CSV file to simulate the click for demo purposes
        const dummyContent = "date,revenue,cost,region\n2023-01-01,1000,500,North\n2023-01-02,1200,600,South";
        const dummyFile = new File([dummyContent], filename, { type: 'text/csv' });
        await handleUpload(dummyFile);
    };

    return (
        <Layout>
            <div className="max-w-4xl mx-auto py-12 px-4 space-y-12 animate-fade-in">
                <header className="text-center space-y-4">
                    <div className="flex justify-center mb-4">
                        {backendHealthy === false && (
                            <span className="px-3 py-1 bg-red-900/30 text-red-400 border border-red-500/30 rounded-full text-[10px] font-bold uppercase tracking-widest animate-pulse">
                                Analytical Engine Offline
                            </span>
                        )}
                        {backendHealthy === true && (
                            <span className="px-3 py-1 bg-green-900/30 text-green-400 border border-green-500/30 rounded-full text-[10px] font-bold uppercase tracking-widest">
                                Analytical Engine Active
                            </span>
                        )}
                    </div>
                    <h1 className="text-4xl font-bold tracking-tight text-white">
                        Upload Data
                    </h1>
                    <p className="text-slate-400 text-lg max-w-lg mx-auto">
                        Connect your CSV dataset and choose how you want to explore it.
                    </p>
                </header>

                <div 
                    className="relative cursor-pointer"
                    onDragOver={(e) => { e.preventDefault(); setIsDragActive(true); }}
                    onDragLeave={() => setIsDragActive(false)}
                    onDrop={(e) => { e.preventDefault(); setIsDragActive(false); if (e.dataTransfer.files?.[0]) handleFile(e.dataTransfer.files[0]); }}
                    onClick={() => fileInputRef.current?.click()}
                >
                    <GlassCard className={`py-20 border-dashed border-2 flex flex-col items-center justify-center text-center space-y-6 ${isDragActive ? 'border-blue-500 bg-blue-900/10' : 'border-slate-800 hover:border-slate-600 transition-colors'}`}>
                        <div className="w-16 h-16 bg-blue-600 rounded-2xl flex items-center justify-center">
                            <svg className="w-8 h-8 text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                            </svg>
                        </div>
                        
                        <div className="space-y-1">
                            <h3 className="text-xl font-bold text-white">{file ? file.name : "Select CSV Dataset"}</h3>
                            <p className="text-slate-500 text-sm">Drag and drop your file here, or click to browse</p>
                        </div>
                    </GlassCard>
                    <input 
                        type="file" 
                        ref={fileInputRef}
                        className="hidden" 
                        onChange={(e) => { if (e.target.files?.[0]) handleFile(e.target.files[0]); }}
                        accept=".csv"
                    />
                </div>

                <div className="flex justify-center">
                    <div className="w-full max-w-xl space-y-4">
                        <div className="bg-slate-900 border border-slate-800 rounded-xl p-2 grid grid-cols-3 gap-2">
                            {(["dashboard", "query", "both"] as const).map((m) => (
                                <button
                                    key={m}
                                    onClick={() => setMode(m)}
                                    className={`h-10 rounded-lg text-xs font-bold uppercase tracking-widest transition-all ${
                                        mode === m
                                            ? "bg-blue-600 text-white shadow-md"
                                            : "text-slate-400 hover:text-white hover:bg-slate-800"
                                    }`}
                                >
                                    {m}
                                </button>
                            ))}
                        </div>
                        <p className="text-xs text-slate-400 text-center">
                            {mode === "dashboard" && "Dashboard only: instant KPIs, charts, insights. Query input will be disabled."}
                            {mode === "query" && "Query only: chat-first exploration with no default dashboard generation."}
                            {mode === "both" && "Both: generate dashboard on upload and keep full query exploration enabled."}
                        </p>
                        <button
                            onClick={() => handleUpload()}
                            disabled={!file || loading}
                            className="w-full px-12 h-14 rounded-xl bg-blue-600 text-white font-bold text-sm shadow-lg hover:bg-blue-500 transition-all disabled:opacity-50 disabled:cursor-not-allowed uppercase tracking-widest"
                        >
                            {loading ? "Processing..." : `Start Analysis (${mode})`}
                        </button>
                    </div>
                </div>

                {/* History placeholder */}
                <div className="pt-12">
                     <div className="flex items-center space-x-4 mb-8">
                        <h2 className="text-[10px] uppercase tracking-[0.2em] text-slate-600 font-bold">Recent Datasets</h2>
                        <div className="h-[1px] flex-1 bg-slate-800"></div>
                     </div>
                     <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <button 
                            onClick={() => handleRecentClick('sample_sales_data.csv')}
                            className="w-full text-left"
                        >
                            <GlassCard className="p-4 flex items-center space-x-4 hover:border-blue-500/50 transition-all cursor-pointer group">
                                <div className="w-10 h-10 rounded-lg bg-slate-800 flex items-center justify-center group-hover:bg-blue-600 transition-colors">
                                    <svg className="w-5 h-5 text-slate-500 group-hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
                                </div>
                                <div className="flex-1">
                                    <p className="font-bold text-sm text-slate-300">sample_sales_data.csv</p>
                                    <p className="text-[9px] text-slate-600 font-bold uppercase tracking-widest mt-0.5">Ready for review</p>
                                </div>
                            </GlassCard>
                        </button>
                        <button 
                            onClick={() => handleRecentClick('inventory_q1.csv')}
                            className="w-full text-left"
                        >
                            <GlassCard className="p-4 flex items-center space-x-4 hover:border-blue-500/50 transition-all cursor-pointer group">
                                <div className="w-10 h-10 rounded-lg bg-slate-800 flex items-center justify-center group-hover:bg-blue-600 transition-colors">
                                    <svg className="w-5 h-5 text-slate-500 group-hover:text-white" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
                                </div>
                                <div className="flex-1">
                                    <p className="font-bold text-sm text-slate-300">inventory_q1.csv</p>
                                    <p className="text-[9px] text-slate-600 font-bold uppercase tracking-widest mt-0.5">Processed 1d ago</p>
                                </div>
                            </GlassCard>
                        </button>
                     </div>
                </div>
            </div>
        </Layout>
    );
};

export default UploadPage;
