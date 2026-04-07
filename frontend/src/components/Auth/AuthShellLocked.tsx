import React from "react";

type AuthShellLockedProps = {
  children: React.ReactNode;
  rightNav?: React.ReactNode;
};

const AuthShellLocked: React.FC<AuthShellLockedProps> = ({ children, rightNav }) => {
  return (
    <div className="min-h-screen w-full bg-[#0f111c] text-[#d9dceb] font-['Inter'] relative overflow-hidden">
      <div className="absolute inset-0 opacity-20 pointer-events-none bg-[radial-gradient(circle_at_1px_1px,#6b7280_1px,transparent_1px)] bg-[size:18px_18px]" />

      <div className="p-4 md:p-8 relative z-10">
        <div
          className="relative mx-auto w-full max-w-[1280px] min-h-[92vh] rounded-2xl border border-[#6f65f5] bg-[radial-gradient(circle_at_30%_20%,rgba(60,90,180,0.25),transparent_35%),radial-gradient(circle_at_75%_80%,rgba(90,30,180,0.25),transparent_35%),#121429] overflow-hidden shadow-[0_30px_120px_rgba(0,0,0,0.55)]"
          style={{ display: "grid", gridTemplateRows: "74px 1fr 78px" }}
        >
          <header className="h-[74px] px-8 flex items-center justify-between border-b border-white/5">
            <div className="text-[40px] leading-none font-black tracking-tighter text-[#f3f5ff]">
              <span>Talking</span>
              <span className="text-[#8fc4ff]">BI</span>
            </div>
            {rightNav ? <div className="hidden md:flex items-center gap-10">{rightNav}</div> : <div />}
          </header>

          <main className="px-4 py-8 flex items-center justify-center">
            <div className="w-full max-w-[420px] mx-auto">{children}</div>
          </main>

          <footer className="h-[78px] border-t border-white/5 px-8 flex flex-col md:flex-row items-center justify-between gap-3 bg-[#0d1020]/80 backdrop-blur-sm">
            <div className="text-xs uppercase tracking-[0.08em] font-semibold text-[#9ca3b5]">
              © 2024 TALKINGBI. ALL RIGHTS RESERVED.
            </div>
            <div className="flex items-center gap-8 text-xs uppercase tracking-[0.08em] font-semibold text-[#a6adbf]">
              <button className="hover:text-white transition-colors">PRIVACY POLICY</button>
              <button className="hover:text-white transition-colors">TERMS OF SERVICE</button>
              <button className="hover:text-white transition-colors">HELP CENTER</button>
            </div>
          </footer>
        </div>
      </div>
    </div>
  );
};

export default AuthShellLocked;

