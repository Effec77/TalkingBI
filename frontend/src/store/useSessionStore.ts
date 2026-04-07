import { create } from 'zustand';

interface SessionState {
  id: string | null;
  mode: 'dashboard' | 'query' | 'both';
  datasetSummary: any | null;
  dashboard: {
    kpis: any[];
    charts: any[];
    insights: any[];
    primary_insight?: string;
  } | null;
  suggestions: string[];
  setSession: (data: any) => void;
  clearSession: () => void;
}

export const useSessionStore = create<SessionState>((set) => ({
  id: null,
  mode: 'both',
  datasetSummary: null,
  dashboard: null,
  suggestions: [],
  setSession: (data) => set({
    id: data.dataset_id,
    mode: data.mode || "both",
    datasetSummary: data.dataset_summary,
    dashboard: data.dashboard,
    suggestions: Array.isArray(data.suggestions)
      ? data.suggestions
      : (data.suggestions?.items || [])
  }),
  clearSession: () => set({
    id: null,
    mode: 'both',
    datasetSummary: null,
    dashboard: null,
    suggestions: []
  }),
}));
