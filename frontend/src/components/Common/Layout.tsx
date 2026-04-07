import React from 'react';
import { useAuthStore } from '../../store/useAuthStore';
import { useNavigate } from 'react-router-dom';

interface LayoutProps {
  children: React.ReactNode;
  showSidebar?: boolean;
}

const Layout: React.FC<LayoutProps> = ({ children, showSidebar = true }) => {
  const { user, logout } = useAuthStore();
  const navigate = useNavigate();

  const handleLogout = () => {
    logout();
    navigate('/login');
  };

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_20%_0%,rgba(59,130,246,0.12),transparent_28%),radial-gradient(circle_at_90%_90%,rgba(99,102,241,0.16),transparent_32%),#020617] text-slate-200 font-sans selection:bg-blue-500/30">
      <div className="flex min-h-screen">
        {showSidebar && (
          <aside className="w-64 border-r border-slate-800/80 bg-slate-900/70 backdrop-blur-md p-8 flex flex-col hidden lg:flex shrink-0">
            <div className="mb-12">
              <h1 className="text-xl font-bold text-white tracking-tight">
                TalkingBI
              </h1>
              <p className="text-xs uppercase tracking-[0.2em] text-slate-500 mt-2">Phase 11</p>
            </div>

            <nav className="flex-1 space-y-1">
              <button 
                onClick={() => navigate('/')}
                className="w-full text-left px-4 py-3 rounded-lg hover:bg-slate-800/90 transition-colors text-slate-400 hover:text-white font-medium text-sm"
              >
                Upload Dataset
              </button>
              <button 
                onClick={() => navigate('/workspace')}
                className="w-full text-left px-4 py-3 rounded-lg hover:bg-slate-800/90 transition-colors text-slate-400 hover:text-white font-medium text-sm"
              >
                Analytics Hub
              </button>
              <button 
                onClick={() => navigate('/settings')}
                className="w-full text-left px-4 py-3 rounded-lg hover:bg-slate-800/90 transition-colors text-slate-400 hover:text-white font-medium text-sm"
              >
                User Settings
              </button>
            </nav>

            <div className="pt-6 border-t border-slate-800">
              <div className="flex items-center space-x-3 px-2 mb-6">
                <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center text-xs font-bold text-white">
                  {user?.email?.[0]?.toUpperCase() || 'U'}
                </div>
                <div className="flex-1 overflow-hidden">
                  <p className="text-sm font-bold text-white truncate">{user?.email}</p>
                  <p className="text-[10px] text-slate-500 uppercase tracking-widest leading-none mt-1">{user?.role || 'User'}</p>
                </div>
              </div>
              <button 
                onClick={handleLogout}
                className="w-full px-4 py-2 text-xs font-bold text-red-400 hover:bg-red-900/20 rounded-lg transition-colors text-left uppercase tracking-widest"
              >
                Sign Out
              </button>
            </div>
          </aside>
        )}

        <main className="flex-1 overflow-y-auto px-4 py-6 lg:px-8 lg:py-8">
          <div className="w-full mx-auto">
            {children}
          </div>
        </main>
      </div>
    </div>
  );
};

export default Layout;
