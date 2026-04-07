import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface SessionState {
  id: string | null;
  mode: 'dashboard' | 'query' | 'both';
  datasetSummary: any | null;
  profile: Record<string, any> | null;
  dashboard: {
    kpis: any[];
    charts: any[];
    insights: any[];
    primary_insight?: string;
  } | null;
  suggestions: string[];
  setSession: (data: any) => void;
  setSuggestions: (items: string[]) => void;
  clearSession: () => void;
}

export const useSessionStore = create<SessionState>()(
  persist(
    (set) => ({
      id: null,
      mode: 'both',
      datasetSummary: null,
      profile: null,
      dashboard: null,
      suggestions: [],
      setSession: (data) => set({
        id: data.dataset_id,
        mode: data.mode || "both",
        datasetSummary: data.dataset_summary,
        profile: data.profile || null,
        dashboard: data.dashboard,
        suggestions: Array.isArray(data.suggestions)
          ? data.suggestions
          : (data.suggestions?.items || [])
      }),
      setSuggestions: (items) => set({
        suggestions: Array.isArray(items) ? items : []
      }),
      clearSession: () => set({
        id: null,
        mode: 'both',
        datasetSummary: null,
        profile: null,
        dashboard: null,
        suggestions: []
      }),
    }),
    {
      name: 'talking-bi-session',
    }
  )
);
