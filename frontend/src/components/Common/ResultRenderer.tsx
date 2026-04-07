import ChartRenderer from "./ChartRenderer";
import { useQueryStore } from "../../store/useQueryStore";
import { useSessionStore } from "../../store/useSessionStore";
import { runQuery } from "../../services/queryService";
import TracePanel from "./TracePanel";
import StatusBanner from "./StatusBanner";

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

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <StatusBanner status={result.status} />
        {result.trace && <TracePanel trace={result.trace} />}
      </div>

      <div className="space-y-6">
        {result.status === "RESOLVED" && (
            <div className="space-y-6">
                {result.charts?.map((c: any, i: number) => (
                    <div key={i} className="animate-fade-in" style={{ animationDelay: `${i * 100}ms` }}>
                         <ChartRenderer chart={c} />
                    </div>
                ))}
                {result.insights && (
                    <ul className="space-y-4 pt-4 border-t border-[#414751]/10">
                        {result.insights.map((insight: string, idx: number) => (
                        <li key={idx} className="flex space-x-3 items-start group">
                            <span className="w-1.5 h-1.5 rounded-full bg-[#fbbb45] mt-2 group-hover:scale-150 transition-transform"></span>
                            <span className="text-sm text-[#c1c7d2] group-hover:text-white transition-colors">{insight}</span>
                        </li>
                        ))}
                    </ul>
                )}
            </div>
        )}

        {(result.status === "INCOMPLETE" || result.status === "AMBIGUOUS") && (
            <div className="p-6 bg-[#ffb4ab]/5 border border-[#ffb4ab]/20 rounded-2xl space-y-4">
                <div className="flex items-center space-x-3 text-[#ffb4ab]">
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                    <h4 className="font-bold uppercase tracking-widest text-[10px]">Follow-up Required</h4>
                </div>
                <h3 className="text-xl font-light">{result.status === "INCOMPLETE" ? "Need more info" : "Which one did you mean?"}</h3>
                <div className="flex flex-wrap gap-2 pt-2">
                    {result.suggestions?.items?.map((s: string, i: number) => (
                        <button 
                            key={i} 
                            onClick={() => handleSuggestion(s)}
                            className="px-4 py-2 bg-[#ffb4ab]/10 hover:bg-[#ffb4ab]/20 border border-[#ffb4ab]/20 rounded-full text-xs transition-all"
                        >
                            {s}
                        </button>
                    ))}
                </div>
            </div>
        )}

        {result.status === "MODE_BLOCKED" && (
            <div className="p-6 bg-[#414751]/10 border border-[#414751]/20 rounded-2xl flex items-center space-x-4">
                 <div className="w-10 h-10 rounded-full bg-[#414751]/20 flex items-center justify-center text-[#ffb4ab]">
                    <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 15v2m0 0v2m0-2h2m-2 0H8m13 0a9 9 0 11-18 0 9 9 0 0118 0z" /></svg>
                 </div>
                 <div>
                    <h4 className="text-[10px] font-bold uppercase tracking-widest text-[#c1c7d2]/40">System Limitation</h4>
                    <p className="text-sm text-[#ffb4ab]/80">{result.message}</p>
                 </div>
            </div>
        )}
      </div>
    </div>
  );
}
