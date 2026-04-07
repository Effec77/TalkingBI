export default function TracePanel({ trace }: { trace: any }) {
  if (!trace?.available) return null;

  return (
    <details className="group border-t border-[#414751]/10 pt-4">
      <summary className="text-[10px] font-bold uppercase tracking-widest text-[#c1c7d2]/40 hover:text-[#a0caff] cursor-pointer transition-colors list-none flex items-center space-x-2">
        <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
        <span>Advanced (How this was computed)</span>
      </summary>
      <div className="mt-4 p-4 bg-[#0d0d18] rounded-xl font-mono text-[11px] text-[#a0caff]/70 overflow-x-auto border border-[#414751]/20">
        <pre>{JSON.stringify(trace.data, null, 2)}</pre>
      </div>
    </details>
  );
}
