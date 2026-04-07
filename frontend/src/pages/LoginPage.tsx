import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import { useAuthStore } from "../store/useAuthStore";
import { authService } from "../services/authService";

const LoginPage: React.FC = () => {
    const [isLogin, setIsLogin] = useState(true);
    const [isReset, setIsReset] = useState(false);
    const [email, setEmail] = useState("");
    const [password, setPassword] = useState("");
    const [fullName, setFullName] = useState("");
    const [loading, setLoading] = useState(false);

    const navigate = useNavigate();
    const setAuth = useAuthStore((state) => state.setAuth);

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault();
        setLoading(true);
        try {
            if (isReset) {
                alert("Reset link sent! Trace the signal in your inbox.");
                setIsReset(false);
                return;
            }

            let response;
            if (isLogin) {
                response = await authService.login({ email, password });
            } else {
                response = await authService.register({ email, password });
            }

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
        <div className="min-h-screen flex items-center justify-center bg-gray-100 dark:bg-gray-900 text-gray-900 dark:text-gray-100 font-sans">
            {/* Background Atmospheric Lighting */}


            <header className="w-full flex justify-center py-4">
                <div className="text-2xl font-bold text-gray-900 dark:text-white">TalkingBI</div>
            </header>

            <main className="flex-1 flex flex-col items-center justify-center p-6 relative z-10">
                <div className="w-full max-w-md bg-white dark:bg-gray-800 rounded-lg shadow-lg p-6">
                    {/* Header */}
                    <div className="mb-10 text-center">
                        <h1 className="text-2xl font-extrabold mb-3 text-gray-900 dark:text-white">
                            {isReset ? "Reset Password" : isLogin ? "Welcome Back" : "Create Account"}
                        </h1>
                        <p className="text-gray-600 dark:text-gray-300 text-sm mb-4">
                            {isReset ? "Enter your email address and we'll send you a link to reset your password." : 
                             isLogin ? "Sign in to your intelligent analytics hub." : 
                             "Join the next generation of conversational intelligence."}
                        </p>
                    </div>

                    <form onSubmit={handleSubmit} className="w-full space-y-6">
                        {!isLogin && !isReset && (
                            <div className="space-y-2">
                                <label className="font-sans text-[10px] uppercase tracking-[0.1em] text-[#8b919c] ml-1">Full Name</label>
                                <div className="relative group">
                                    <div className="absolute inset-y-0 left-4 flex items-center pointer-events-none text-[#8b919c] group-focus-within:text-[#a0caff] transition-colors">
                                        <span className="material-symbols-outlined text-[20px]">person</span>
                                    </div>
                                    <input
                                        type="text"
                                        value={fullName}
                                        onChange={(e) => setFullName(e.target.value)}
                                        placeholder="Alex Rivera"
                                        className="w-full bg-[#343440] border-none text-white text-sm py-4 pl-12 pr-4 rounded-xl focus:ring-1 focus:ring-[#a0caff]/50 transition-all placeholder:text-slate-500 font-medium"
                                    />
                                </div>
                            </div>
                        )}

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

                        {!isReset && (
                            <div className="space-y-2">
                                <div className="flex justify-between items-center">
                                    <label className="font-sans text-[10px] uppercase tracking-[0.1em] text-[#8b919c] ml-1">Password</label>
                                    {isLogin && (
                                        <button 
                                            type="button" 
                                            onClick={() => setIsReset(true)}
                                            className="text-[10px] font-bold text-[#a0caff] hover:text-white transition-colors"
                                        >
                                            Forgot Password?
                                        </button>
                                    )}
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
                                        className="w-full bg-[#343440] border-none text-white text-sm py-4 pl-12 pr-4 rounded-xl focus:ring-1 focus:ring-[#a0caff]/50 transition-all placeholder:text-slate-500 font-medium"
                                    />
                                </div>
                            </div>
                        )}

                        <button
                            type="submit"
                            disabled={loading}
                            className="w-full bg-gradient-to-br from-[#5203d5] to-[#4f94dd] text-white font-bold py-4 rounded-xl shadow-[0_0_20px_rgba(79,148,221,0.2)] hover:shadow-[0_0_30px_rgba(79,148,221,0.4)] transition-all active:scale-[0.98] flex items-center justify-center gap-2"
                        >
                            <span>{loading ? "Decrypting..." : isReset ? "Send Reset Link" : isLogin ? "Sign In" : "Sign Up"}</span>
                            <span className="material-symbols-outlined text-[18px]">arrow_forward</span>
                        </button>
                    </form>

{/* Navigation Link */}
<div className="flex flex-col space-y-4 mt-6">
  <button className="flex items-center justify-center w-full bg-white text-[#12121d] py-2 rounded-xl shadow-md hover:shadow-lg transition-shadow">
    <span className="material-symbols-outlined mr-2">google</span> Sign in with Google
  </button>
  <button className="flex items-center justify-center w-full bg-[#333] text-white py-2 rounded-xl shadow-md hover:shadow-lg transition-shadow">
    <span className="material-symbols-outlined mr-2">github</span> Sign in with GitHub
  </button>
</div>
                    <div className="mt-8 pt-8 border-t border-white/5 w-full text-center">
                        {isReset ? (
                            <button 
                                onClick={() => setIsReset(false)}
                                className="inline-flex items-center gap-2 text-sm text-slate-400 hover:text-[#a0caff] transition-colors group"
                            >
                                <span className="material-symbols-outlined text-[18px] group-hover:-translate-x-1 transition-transform">arrow_back</span>
                                <span>Back to Login</span>
                            </button>
                        ) : (
                            <p className="text-sm font-medium text-slate-400">
                                {isLogin ? "Don't have an account?" : "Already have an account?"}{" "}
                                <button 
                                    onClick={() => { setIsLogin(!isLogin); setIsReset(false); }}
                                    className="text-[#a0caff] hover:text-white font-bold ml-1 transition-colors"
                                >
                                    {isLogin ? "Create account" : "Sign in"}
                                </button>
                            </p>
                        )}
                    </div>
                </div>

                {/* System State Decorator */}
                <div className="mt-12 flex justify-center items-center gap-4 opacity-40">
                    <span className="font-mono text-[10px] tracking-widest uppercase text-[#8b919c]">System.Auth.Provider</span>
                    <div className="h-[1px] w-8 bg-white/10 text-transparent">.</div>
                    <div className="flex gap-1">
                        <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/40"></div>
                        <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/20"></div>
                        <div className="w-1.5 h-1.5 rounded-full bg-[#a0caff]/10"></div>
                    </div>
                </div>
            </main>

            {/* Footer */}
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

export default LoginPage;
