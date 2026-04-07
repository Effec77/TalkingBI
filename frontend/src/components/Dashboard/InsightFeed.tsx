import React from 'react';
import GlassCard from '../Common/GlassCard';

interface InsightItem {
    query: string;
    insight: string;
    timestamp?: string;
    type?: 'trend' | 'outlier' | 'general' | 'comparison';
}

const InsightFeed: React.FC<{ items: InsightItem[] }> = ({ items }) => {
    if (!items?.length) return null;

    return (
        <div className="space-y-4">
             <div className="flex items-center space-x-4 mb-6">
                <h3 className="text-sm uppercase tracking-widest font-bold text-[#e3e0f1]/40">Intelligence Narrative</h3>
                <div className="h-[1px] flex-1 bg-[#414751]/10"></div>
             </div>
             
             <div className="space-y-3">
                {items.map((item, i) => (
                    <GlassCard key={i} className="hover:bg-[#a0caff]/5 transition-all group border-l-4 border-l-[#a0caff]/20 hover:border-l-[#a0caff]">
                        <div className="flex justify-between items-start mb-2">
                             <span className="text-[10px] font-bold uppercase tracking-widest text-[#a0caff] opacity-60">Result from: {item.query}</span>
                             {item.timestamp && <span className="text-[10px] text-[#c1c7d2]/30 font-mono tracking-widest uppercase">{item.timestamp}</span>}
                        </div>
                        <p className="text-sm leading-relaxed text-[#c1c7d2] group-hover:text-[#e3e0f1] transition-colors">{item.insight}</p>
                    </GlassCard>
                ))}
             </div>
        </div>
    );
};

export default InsightFeed;
