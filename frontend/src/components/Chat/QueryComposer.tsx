import React from "react";
import { useSessionStore } from "../../store/useSessionStore";
import { useQueryStore } from "../../store/useQueryStore";
import { logEvent } from "../../utils/logger";
import { runQuery } from "../../services/queryService";

interface QueryComposerProps {
    borderNone?: boolean;
}

export default function QueryComposer({ borderNone = false }: QueryComposerProps) {
  const input = useQueryStore((s) => s.input);
  const setInput = useQueryStore((s) => s.setInput);
  
  const sessionId = useSessionStore((s) => s.id);
  const setSuggestions = useSessionStore((s) => s.setSuggestions);
  const addToHistory = useQueryStore((s) => s.addToHistory);
  const setLoading = useQueryStore((s) => s.setLoading);
  const loading = useQueryStore((s) => s.loading);

  const handleQuery = async (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    if (!input || !sessionId || loading) return;

    setLoading(true);

    try {
      const res = await runQuery(sessionId, input);
      logEvent('QUERY_SUCCESS', { query: input, status: res.status });

      addToHistory({
        query: input,
        response: res,
        status: res.status,
      });
      if (Array.isArray(res?.suggestions?.items)) {
        setSuggestions(res.suggestions.items);
      }

      setInput("");
    } catch (err: any) {
      logEvent('QUERY_ERROR', err);
      console.error(err);
    }

    setLoading(false);
  };

  const [isListening, setIsListening] = React.useState(false);

  const startListening = () => {
    const SpeechRecognition = (window as any).SpeechRecognition || (window as any).webkitSpeechRecognition;
    if (!SpeechRecognition) return;

    const recognition = new SpeechRecognition();
    recognition.lang = 'en-US';
    recognition.interimResults = false;
    recognition.maxAlternatives = 1;

    recognition.onstart = () => setIsListening(true);
    recognition.onend = () => setIsListening(false);
    recognition.onerror = () => setIsListening(false);

    recognition.onresult = (event: any) => {
      const transcript = event.results[0][0].transcript;
      setInput(transcript);
    };

    recognition.start();
  };

  return (
    <form 
        onSubmit={handleQuery}
        className={`flex items-center space-x-2 px-4 py-3 bg-transparent ${borderNone ? '' : 'border border-[#cfd8ea] rounded-xl bg-white'}`}
    >
        <div className="flex-1 relative">
            <input
                value={input}
                onChange={(e) => setInput(e.target.value)}
                placeholder="Converse with your dataset..."
                className="w-full bg-transparent border-none focus:ring-0 text-[#17325f] placeholder-[#94a3b8] text-base py-2 pr-12"
                autoFocus
            />
            <button
                type="button"
                onClick={startListening}
                className={`absolute right-0 top-1/2 -translate-y-1/2 p-2 rounded-full transition-all ${
                    isListening 
                    ? 'text-[#2f5597] animate-pulse shadow-[0_0_12px_rgba(47,85,151,0.25)] bg-[#dfe9ff]' 
                    : 'text-[#64748b] hover:text-[#2f5597] hover:bg-[#eff4ff]'
                }`}
            >
                <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 11a7 7 0 01-7 7m0 0a7 7 0 01-7-7m7 7v4m0 0H8m4 0h4m-4-8a3 3 0 01-3-3V5a3 3 0 116 0v6a3 3 0 01-3 3z" />
                </svg>
            </button>
            {loading && (
                <div className="absolute right-12 top-1/2 -translate-y-1/2 flex items-center space-x-2 text-[#2f5597]">
                    <span className="text-[10px] uppercase tracking-widest font-bold animate-pulse">Calculating...</span>
                </div>
            )}
        </div>
        
        <button 
            type="submit"
            disabled={loading || !input} 
            className={`w-12 h-12 rounded-full flex items-center justify-center transition-all ${
                input && !loading 
                ? 'bg-gradient-to-r from-[#4f7fcb] to-[#2f5597] text-white shadow-sm' 
                : 'bg-[#e2e8f0] text-[#94a3b8]'
            }`}
        >
            {loading ? (
                 <svg className="animate-spin h-5 w-5" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                 <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                 <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
             </svg>
            ) : (
                <svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M14 5l7 7m0 0l-7 7m7-7H3" />
                </svg>
            )}
        </button>
    </form>
  );
}
