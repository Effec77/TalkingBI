import { create } from 'zustand';

export interface QueryHistoryItem {
  query: string;
  response: any;
  status: string;
}

interface QueryState {
  input: string;
  loading: boolean;
  history: QueryHistoryItem[];
  lastResult: any | null;
  setInput: (input: string) => void;
  setLoading: (loading: boolean) => void;
  addToHistory: (entry: QueryHistoryItem) => void;
  setLastResult: (result: any) => void;
  clearHistory: () => void;
}

export const useQueryStore = create<QueryState>((set) => ({
  input: '',
  loading: false,
  history: [],
  lastResult: null,
  setInput: (input) => set({ input }),
  setLoading: (loading) => set({ loading }),
  addToHistory: (entry) =>
    set((state) => ({
      history: [...state.history, entry],
      lastResult: entry.response
    })),
  setLastResult: (result) => set({ lastResult: result }),
  clearHistory: () => set({ history: [], lastResult: null })
}));
