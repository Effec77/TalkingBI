import React, { useState } from "react";
import { useNavigate } from "react-router-dom";


const ResetPasswordPage: React.FC = () => {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      // Assuming backend has a password reset endpoint
      const res = await fetch("http://127.0.0.1:8000/auth/reset-password", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email }),
      });
      if (!res.ok) throw new Error("Reset request failed");
      alert("Reset link sent! Check your inbox.");
      navigate("/login");
    } catch (err: any) {
      alert(err.message || "Unexpected error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full bg-[#12121d] text-[#e3e0f1] flex flex-col font-sans selection:bg-[#a0caff]/30 relative overflow-hidden">
      {/* Background */}
      <div className="absolute top-[-10%] right-[-5%] w-[500px] h-[500px] bg-[#4f94dd] opacity-[0.05] blur-[80px] rounded-full pointer-events-none"></div>
      <div className="absolute bottom-[-10%] left-[-5%] w-[600px] h-[600px] bg-[#5203d5] opacity-[0.05] blur-[80px] rounded-full pointer-events-none"></div>

      <nav className="relative z-20 w-full px-8 py-8 flex justify-between items-center bg-[#12121d]">
        <div className="text-2xl font-black tracking-tighter text-white bg-gradient-to-br from-[#5203d5] to-[#4f94dd] bg-clip-text text-transparent">TalkingBI</div>
      </nav>

      <main className="flex-1 flex flex-col items-center justify-center p-6 relative z-10">
        <div className="w-full max-w-[440px] bg-[#1b1a26]/40 backdrop-blur-xl border-t border-white/10 border-l border-white/5 rounded-3xl p-10 sm:p-14 shadow-[0_40px_100px_-20px_rgba(0,0,0,0.5)]">
          <h1 className="text-3xl font-extrabold tracking-tight text-[#e3e0f1] mb-3 text-center">Reset Password</h1>
          <p className="text-[#8b919c] text-sm text-center mb-6">Enter your email address and we’ll send you a link to reset your password.</p>
          <form onSubmit={handleSubmit} className="space-y-6">
            <div className="space-y-2">
              <label className="font-sans text-[10px] uppercase tracking-[0.1em] text-[#8b919c] ml-1">Email address</label>
              <div className="relative group">
                <div className="absolute inset-y-0 left-4 flex items-center pointer-events-none text-[#8b919c] group-focus-within:text-[#a0caff] transition-colors">
                  <span className="material-symbols-outlined text-[20px]">mail</span>
                </div>
                <input
                  type="email"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  required
                  placeholder="name@company.com"
                  className="w-full bg-[#343440] border-none text-white text-sm py-4 pl-12 pr-4 rounded-xl focus:ring-1 focus:ring-[#a0caff]/50 transition-all placeholder:text-slate-500 font-medium"
                />
              </div>
            </div>
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-gradient-to-br from-[#5203d5] to-[#4f94dd] text-white font-bold py-4 rounded-xl shadow-[0_0_20px_rgba(79,148,221,0.2)] hover:shadow-[0_0_30px_rgba(79,148,221,0.4)] transition-all active:scale-[0.98] flex items-center justify-center gap-2"
            >
              {loading ? "Sending..." : "Send Reset Link"}
            </button>
          </form>
          <p className="text-sm font-medium text-slate-400 mt-6 text-center">
            Remembered your password?{' '}
            <button onClick={() => navigate('/login')} className="text-[#a0caff] hover:text-white font-bold transition-colors">
              Sign In
            </button>
          </p>
        </div>
      </main>

      <footer className="w-full border-t border-white/5 bg-[#12121d] flex flex-col md:flex-row justify-between items-center px-12 py-10 gap-4 mt-auto">
        <div className="text-white font-black text-lg tracking-tighter">TalkingBI</div>
        <div className="flex gap-10">
          <button className="text-[10px] uppercase tracking-[0.1em] font-black text-slate-500 hover:text-[#4f94dd] transition-all">Privacy Policy</button>
          <button className="text-[10px] uppercase tracking-[0.1em] font-black text-slate-500 hover:text-[#4f94dd] transition-all">Terms of Service</button>
          <button className="text-[10px] uppercase tracking-[0.1em] font-black text-slate-500 hover:text-[#4f94dd] transition-all">Support</button>
        </div>
        <div className="text-[10px] uppercase tracking-[0.1em] font-black text-slate-500">
          © 2024 TalkingBI Oracle. All rights reserved.
        </div>
      </footer>
    </div>
  );
};

export default ResetPasswordPage;
