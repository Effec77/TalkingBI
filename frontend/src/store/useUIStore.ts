import { create } from 'zustand';

interface UIState {
  isTraceOpen: boolean;
  isChatOpen: boolean;
  activePanel: 'dashboard' | 'chat';
  loadingStates: {
    upload: boolean;
    query: boolean;
  };
  setTraceOpen: (isOpen: boolean) => void;
  setChatOpen: (isOpen: boolean) => void;
  setActivePanel: (panel: 'dashboard' | 'chat') => void;
  setLoading: (type: 'upload' | 'query', isLoading: boolean) => void;
}

export const useUIStore = create<UIState>((set) => ({
  isTraceOpen: false,
  isChatOpen: false,
  activePanel: 'dashboard',
  loadingStates: {
    upload: false,
    query: false,
  },
  setTraceOpen: (isOpen) => set({ isTraceOpen: isOpen }),
  setChatOpen: (isOpen) => set({ isChatOpen: isOpen }),
  setActivePanel: (panel) => set({ activePanel: panel }),
  setLoading: (type, isLoading) => 
    set((state) => ({
      loadingStates: { ...state.loadingStates, [type]: isLoading }
    }))
}));
