import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import AuthShell from "../components/Auth/AuthShellLocked";

const ResetPasswordPageModern: React.FC = () => {
  const [email, setEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
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
    <AuthShell>
      <div className="bg-[rgba(28,31,49,0.88)] border border-white/10 p-8 rounded-3xl shadow-[0_30px_80px_rgba(0,0,0,0.6)] backdrop-blur-md">
        <div className="mb-8 text-center">
          <h1 className="text-4xl font-bold text-white tracking-tight">Reset your password</h1>
          <p className="text-slate-300 mt-2 font-medium">Enter your email and we will send a reset link.</p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-5">
          <div className="space-y-1.5">
            <label className="text-xs font-bold uppercase tracking-widest text-slate-500">Email Address</label>
            <input
              type="email"
              required
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="name@company.com"
              className="w-full h-12 bg-[#090c1e] border border-[#3a4460] rounded-xl px-4 text-white placeholder:text-slate-600 focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-500 transition-all font-medium"
            />
          </div>

          <button
            type="submit"
            disabled={loading}
            className="w-full h-12 bg-gradient-to-r from-[#67a7f3] to-[#6018ee] hover:brightness-110 text-white font-bold rounded-xl shadow-lg shadow-blue-900/30 transition-all disabled:opacity-50"
          >
            {loading ? "Sending..." : "Send Reset Link"}
          </button>
        </form>

        <div className="mt-8 text-center">
          <Link to="/login" className="inline-flex items-center gap-2 text-sm font-bold text-slate-400 hover:text-white transition-colors">
            <span className="material-symbols-outlined text-base">arrow_back</span>
            Back to Login
          </Link>
        </div>
      </div>
    </AuthShell>
  );
};

export default ResetPasswordPageModern;
