import React from 'react';
import GlassCard from '../Common/GlassCard';

interface KPIProps {
    label: string;
    value: string | number;
    description?: string;
    trend?: 'up' | 'down' | 'neutral';
}

const KPIStats: React.FC<{ metrics: KPIProps[] }> = ({ metrics }) => {
    return (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {metrics.map((m, i) => (
                <GlassCard key={i} className="border-slate-800 bg-slate-900 group">
                    <div className="space-y-2 relative z-10">
                        <div className="flex justify-between items-start opacity-40 group-hover:opacity-100 transition-opacity">
                            <h3 className="text-[9px] font-bold uppercase tracking-[0.2em] text-[#e3e0f1]">{m.label}</h3>
                            {m.trend && (
                                <span className={`text-[10px] font-bold ${m.trend === 'up' ? 'text-[#a0caff]' : m.trend === 'down' ? 'text-[#ffb4ab]' : 'text-[#c1c7d2]'}`}>
                                    {m.trend === 'up' ? '↑' : m.trend === 'down' ? '↓' : '→'}
                                </span>
                            )}
                        </div>
                        <p className="text-3xl font-light tracking-tighter text-[#e3e0f1]/90">
                            {m.value}
                        </p>
                        {m.description && <p className="text-[10px] text-[#c1c7d2]/30 truncate font-mono uppercase tracking-widest">{m.description}</p>}
                    </div>
                </GlassCard>
            ))}
        </div>
    );
};

export default KPIStats;
