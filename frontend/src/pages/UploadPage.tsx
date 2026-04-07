import React, { useState, useRef } from "react";
import { uploadCSV } from "../services/uploadService";
import { useSessionStore } from "../store/useSessionStore";
import { useNavigate } from "react-router-dom";
import { logEvent } from "../utils/logger";
import Layout from "../components/Common/Layout";
import GlassCard from "../components/Common/GlassCard";

const UploadPage: React.FC = () => {
    const [file, setFile] = useState<File | null>(null);
    const [isDragActive, setIsDragActive] = useState(false);
    const [loading, setLoading] = useState(false);
    const setSession = useSessionStore((s) => s.setSession);
    const navigate = useNavigate();
    const fileInputRef = useRef<HTMLInputElement>(null);

    const handleFile = (f: File) => {
        if (f.name.endsWith('.csv')) {
            setFile(f);
            logEvent('FILE_SELECTED', { name: f.name });
        } else {
            alert('Please select a CSV file');
        }
    };

    const handleUpload = async () => {
        if (!file) return;

        setLoading(true);
        try {
            const res = await uploadCSV(file, "both");
            logEvent('UPLOAD_SUCCESS', { datasetId: res.dataset_id });
            setSession(res);
            navigate("/workspace");
        } catch (err: any) {
            logEvent('UPLOAD_ERROR', err);
            console.error(err);
            alert(err.message || "Upload failed");
        }
        setLoading(false);
    };

    return (
        <Layout>
            <div className="max-w-4xl mx-auto py-12 px-4 space-y-12 animate-fade-in">
                <header className="text-center space-y-4">
                    <h1 className="text-4xl font-bold tracking-tight text-white">
                        Upload Data
                    </h1>
                    <p className="text-slate-400 text-lg max-w-lg mx-auto">
                        Connect your CSV dataset to begin your analysis.
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
                    <button
                        onClick={handleUpload}
                        disabled={!file || loading}
                        className="px-12 h-14 rounded-xl bg-blue-600 text-white font-bold text-sm shadow-lg hover:bg-blue-500 transition-all disabled:opacity-50 disabled:cursor-not-allowed uppercase tracking-widest"
                    >
                        {loading ? "Processing..." : "Start Analysis"}
                    </button>
                </div>

                {/* History placeholder */}
                <div className="pt-12">
                     <div className="flex items-center space-x-4 mb-8">
                        <h2 className="text-[10px] uppercase tracking-[0.2em] text-slate-600 font-bold">Recent Datasets</h2>
                        <div className="h-[1px] flex-1 bg-slate-800"></div>
                     </div>
                     <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                        <GlassCard className="p-4 flex items-center space-x-4">
                            <div className="w-10 h-10 rounded-lg bg-slate-800 flex items-center justify-center">
                                <svg className="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
                            </div>
                            <div className="flex-1">
                                <p className="font-bold text-sm text-slate-300">sample_sales_data.csv</p>
                                <p className="text-[9px] text-slate-600 font-bold uppercase tracking-widest mt-0.5">Ready for review</p>
                            </div>
                        </GlassCard>
                        <GlassCard className="p-4 flex items-center space-x-4">
                            <div className="w-10 h-10 rounded-lg bg-slate-800 flex items-center justify-center">
                                <svg className="w-5 h-5 text-slate-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" /></svg>
                            </div>
                            <div className="flex-1">
                                <p className="font-bold text-sm text-slate-300">inventory_q1.csv</p>
                                <p className="text-[9px] text-slate-600 font-bold uppercase tracking-widest mt-0.5">Processed 1d ago</p>
                            </div>
                        </GlassCard>
                     </div>
                </div>
            </div>
        </Layout>
    );
};

export default UploadPage;
