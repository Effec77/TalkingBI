import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import { useAuthStore } from "../store/useAuthStore";
import { authService } from "../services/authService";

const LoginPageV2: React.FC = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);

  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      const response = await authService.login({ email, password });
      const user = authService.decodeUserFromToken(response.access_token);
      setAuth(user, response.access_token);
      navigate("/");
    } catch (error: any) {
      alert(error.message || "Identity sync failed. Check your credentials.");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full bg-[#12121d] text-[#e3e0f1] font-['Inter'] flex flex-col overflow-hidden relative">
      <div className="absolute top-[-10%] right-[-5%] w-[500px] h-[500px] rounded-full blur-[80px] opacity-15 bg-[radial-gradient(circle,_#4f94dd_0%,_transparent_70%)] pointer-events-none" />
      <div className="absolute bottom-[-10%] left-[-5%] w-[600px] h-[600px] rounded-full blur-[80px] opacity-10 bg-[radial-gradient(circle,_#5203d5_0%,_transparent_70%)] pointer-events-none" />

      <main className="w-full max-w-md px-6 mx-auto flex-1 flex flex-col items-center justify-center relative z-10">
        <div className="flex justify-center mb-12">
          <span className="text-3xl font-black tracking-tighter text-transparent bg-clip-text bg-gradient-to-br from-[#5203d5] to-[#4f94dd]">
            TalkingBI
          </span>
        </div>

        <div className="w-full bg-[rgba(27,26,38,0.4)] backdrop-blur-xl border-t border-white/10 border-l border-white/5 rounded-xl p-10 shadow-[0_40px_100px_-20px_rgba(0,0,0,0.5)]">
          <div className="mb-8 text-center">
            <h1 className="text-3xl font-extrabold tracking-tight text-[#e3e0f1] mb-3">Welcome Back</h1>
            <p className="text-[#c1c7d2] text-sm leading-relaxed max-w-[280px] mx-auto">
              Sign in to your intelligent analytics hub.
            </p>
          </div>

          <form onSubmit={handleSubmit} className="space-y-6">
            <div className="space-y-2">
              <label className="text-[10px] uppercase tracking-[0.1em] text-[#8b919c] ml-1 font-['Space_Grotesk']">
                Email address
              </label>
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
                  className="w-full bg-[#343440] border-none text-white text-sm py-4 pl-12 pr-4 rounded-xl focus:ring-0 focus:bg-[#383845] transition-all placeholder:text-slate-500"
                />
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <label className="text-[10px] uppercase tracking-[0.1em] text-[#8b919c] ml-1 font-['Space_Grotesk']">
                  Password
                </label>
                <Link to="/reset-password" className="text-[10px] font-bold text-[#a0caff] hover:text-white transition-colors">
                  Forgot Password?
                </Link>
              </div>
              <div className="relative group">
                <div className="absolute inset-y-0 left-4 flex items-center pointer-events-none text-[#8b919c] group-focus-within:text-[#a0caff] transition-colors">
                  <span className="material-symbols-outlined text-[20px]">lock</span>
                </div>
                <input
                  type="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                  placeholder="••••••••"
                  className="w-full bg-[#343440] border-none text-white text-sm py-4 pl-12 pr-4 rounded-xl focus:ring-0 focus:bg-[#383845] transition-all placeholder:text-slate-500"
                />
              </div>
            </div>

            <button
              type="submit"
              disabled={loading}
              className="w-full bg-gradient-to-br from-[#5203d5] to-[#4f94dd] text-white font-bold py-4 rounded-xl shadow-[0_0_20px_rgba(79,148,221,0.2)] hover:shadow-[0_0_30px_rgba(79,148,221,0.4)] transition-all active:scale-[0.98] flex items-center justify-center gap-2"
            >
              <span>{loading ? "Signing in..." : "Sign In"}</span>
              <span className="material-symbols-outlined text-[18px]">arrow_forward</span>
            </button>
          </form>

          <div className="mt-8 pt-8 border-t border-white/5 text-center">
            <p className="text-sm text-slate-400">
              Don't have an account?{" "}
              <Link to="/register" className="text-[#a0caff] hover:text-white font-bold transition-colors">
                Create account
              </Link>
            </p>
          </div>
        </div>

        <div className="mt-8 flex justify-center items-center gap-4 opacity-40">
          <span className="font-['JetBrains_Mono'] text-[10px] tracking-widest uppercase text-[#8b919c]">
            System.Auth.Provider
          </span>
          <div className="h-[1px] w-8 bg-[#414751]" />
          <div className="flex gap-1">
            <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/40" />
            <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/20" />
            <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/10" />
          </div>
        </div>
      </main>

      <footer className="w-full border-t border-white/5 bg-[#12121d] flex flex-col md:flex-row justify-between items-center px-8 py-8 gap-4 mt-auto">
        <div className="text-white font-bold">TalkingBI</div>
        <div className="flex gap-8">
          <button className="text-xs uppercase tracking-[0.05em] text-slate-500 hover:text-[#4f94dd] transition-all">
            Privacy Policy
          </button>
          <button className="text-xs uppercase tracking-[0.05em] text-slate-500 hover:text-[#4f94dd] transition-all">
            Terms of Service
          </button>
          <button className="text-xs uppercase tracking-[0.05em] text-slate-500 hover:text-[#4f94dd] transition-all">
            Support
          </button>
        </div>
        <div className="text-xs uppercase tracking-[0.05em] text-slate-500">
          © 2024 TalkingBI Oracle. All rights reserved.
        </div>
      </footer>
    </div>
  );
};

export default LoginPageV2;

