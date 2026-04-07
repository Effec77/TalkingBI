import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import AuthShell from "../components/Auth/AuthShellLocked";
import { useAuthStore } from "../store/useAuthStore";
import { authService } from "../services/authService";

const LoginPageModern: React.FC = () => {
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [usingOfflineMode, setUsingOfflineMode] = useState(false);

  const navigate = useNavigate();
  const setAuth = useAuthStore((state) => state.setAuth);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      if (email.trim().toLowerCase() === "demo@talkingbi.local" && password === "demo123") {
        setUsingOfflineMode(true);
        setAuth(
          {
            id: "demo-user",
            email: "demo@talkingbi.local",
            role: "admin",
            org_id: null,
          },
          "demo-token"
        );
        navigate("/");
        return;
      }

      const response = await authService.login({ email, password });
      const user = authService.decodeUserFromToken(response.access_token);
      setAuth(user, response.access_token);
      try {
        const me = await authService.me();
        setAuth(
          { id: me.id, email: me.email, role: me.role, org_id: me.org_id, display_name: me.display_name ?? null, avatar_url: me.avatar_url ?? null },
          response.access_token
        );
      } catch {
        // fallback to decoded token
      }
      navigate("/");
    } catch (error: any) {
      const message = String(error?.message || "");
      if (
        message.toLowerCase().includes("no response from server") ||
        message.toLowerCase().includes("network") ||
        message.toLowerCase().includes("failed to fetch")
      ) {
        setUsingOfflineMode(true);
        setAuth(
          {
            id: "demo-user",
            email: email || "demo@talkingbi.local",
            role: "admin",
            org_id: null,
          },
          "demo-token"
        );
        navigate("/");
        return;
      }
      alert(message || "Sign in failed");
    } finally {
      setLoading(false);
    }
  };

  const handleDemoAccess = () => {
    setUsingOfflineMode(true);
    setAuth(
      {
        id: "demo-user",
        email: "demo@talkingbi.local",
        role: "admin",
        org_id: null,
      },
      "demo-token"
    );
    navigate("/");
  };

  return (
    <AuthShell>
      <div className="bg-[rgba(28,31,49,0.88)] border border-white/10 p-8 rounded-3xl shadow-[0_30px_80px_rgba(0,0,0,0.6)] backdrop-blur-md">
        <div className="mb-8 text-center">
          <h1 className="text-4xl font-bold text-white tracking-tight">Welcome Back</h1>
          <p className="text-slate-300 mt-2 font-medium">Sign in to your intelligent analytics hub.</p>
          {usingOfflineMode && (
            <p className="mt-2 text-xs font-bold text-amber-400 uppercase tracking-widest">Running in Demo Mode</p>
          )}
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          <div className="space-y-1.5">
            <label className="text-xs font-bold uppercase tracking-widest text-slate-400">Email Address</label>
            <input
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="name@company.com"
              className="w-full h-12 bg-[#090c1e] border border-[#3a4460] rounded-xl px-4 text-white placeholder:text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium"
            />
          </div>

          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-xs font-bold uppercase tracking-widest text-slate-400">Password</label>
              <Link to="/reset-password" id="forgot-password-link" className="text-xs font-bold text-blue-400 hover:text-blue-300 transition-colors">
                Forgot?
              </Link>
            </div>
            <input
              type="password"
              required
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="••••••••"
              className="w-full h-12 bg-slate-950 border border-slate-800 rounded-lg px-4 text-white placeholder:text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full h-12 bg-gradient-to-r from-[#67a7f3] to-[#6018ee] hover:brightness-110 text-white font-bold rounded-xl shadow-lg shadow-blue-900/30 transition-all disabled:opacity-50"
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>

        <div className="mt-8 text-center text-slate-400 text-sm font-medium">
          New here?{" "}
          <Link to="/register" className="text-blue-400 font-bold hover:text-blue-300 transition-colors">
            Create account
          </Link>
        </div>

        <div className="mt-8">
          <div className="relative">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-white/10"></div>
            </div>
            <span className="relative px-3 bg-[rgba(28,31,49,0.88)] text-[10px] uppercase tracking-widest font-bold text-slate-500">
              Other Options
            </span>
          </div>

          <div className="mt-6 grid grid-cols-2 gap-3">
            <button
              type="button"
              onClick={() => authService.startOAuth("google")}
              className="h-11 flex items-center justify-center bg-[#181d33] border border-white/10 text-slate-300 text-sm font-bold rounded-xl hover:bg-[#1d2440] transition-colors"
            >
              Google
            </button>
            <button
              type="button"
              onClick={() => authService.startOAuth("github")}
              className="h-11 flex items-center justify-center bg-[#181d33] border border-white/10 text-slate-300 text-sm font-bold rounded-xl hover:bg-[#1d2440] transition-colors"
            >
              GitHub
            </button>
          </div>

          <button
            type="button"
            onClick={handleDemoAccess}
            className="w-full mt-4 h-11 flex items-center justify-center border border-white/10 text-slate-400 text-xs font-bold uppercase tracking-widest rounded-xl hover:bg-[#1d2440] transition-colors"
          >
            Skip to Demo
          </button>
          <p className="mt-3 text-center text-[11px] text-slate-500">Demo: demo@talkingbi.local / demo123</p>
        </div>
      </div>

      <div className="mt-8 text-center">
        <span className="text-[10px] font-bold tracking-[0.2em] uppercase text-slate-600">
          Intelligent BI Workspace Alpha
        </span>
      </div>
    </AuthShell>
  );
};

export default LoginPageModern;
