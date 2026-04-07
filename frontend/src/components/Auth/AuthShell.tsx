import React from "react";

type AuthShellProps = {
  children: React.ReactNode;
  rightNav?: React.ReactNode;
};

const AuthShell: React.FC<AuthShellProps> = ({ children, rightNav }) => {
  return (
    <div className="min-h-screen w-full bg-[#0f172a] text-slate-200 font-sans flex items-center justify-center p-4">
      <div className="w-full max-w-[1100px] flex flex-col items-center">
        <header className="w-full flex items-center justify-between mb-8 px-4">
          <div className="text-2xl font-bold tracking-tight text-white">
            Talking<span className="text-blue-400">BI</span>
          </div>
          {rightNav && <div className="hidden md:flex items-center gap-6">{rightNav}</div>}
        </header>

        <main className="w-full flex items-center justify-center">
          <div className="w-full max-w-[440px]">{children}</div>
        </main>

        <footer className="w-full mt-12 pt-6 border-t border-slate-800 flex flex-col md:flex-row items-center justify-between gap-4 text-slate-500 text-xs font-medium uppercase tracking-widest">
          <div>© 2024 TalkingBI</div>
          <div className="flex items-center gap-6">
            <button className="hover:text-slate-300 transition-colors">Privacy</button>
            <button className="hover:text-slate-300 transition-colors">Terms</button>
            <button className="hover:text-slate-300 transition-colors">Support</button>
          </div>
        </footer>
      </div>
    </div>
  );
};

export default AuthShell;
