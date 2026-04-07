export default function TracePanel({ trace }: { trace: any }) {
  if (!trace?.available) return null;

  return (
    <details className="group border-t border-[#dbe4f3] pt-4">
      <summary className="text-[10px] font-bold uppercase tracking-widest text-[#64748b] hover:text-[#2f5597] cursor-pointer transition-colors list-none flex items-center space-x-2">
        <svg className="w-3 h-3 transition-transform group-open:rotate-90" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" /></svg>
        <span>Advanced (How this was computed)</span>
      </summary>
      <div className="mt-4 p-4 bg-[#f7f9fd] rounded-xl font-mono text-[11px] text-[#334155] overflow-x-auto border border-[#dbe4f3]">
        <pre>{JSON.stringify(trace.data, null, 2)}</pre>
      </div>
    </details>
  );
}
