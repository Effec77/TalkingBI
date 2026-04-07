import React, { useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useAuthStore } from "../store/useAuthStore";
import { authService } from "../services/authService";

const AuthCallbackPage: React.FC = () => {
  const [params] = useSearchParams();
  const navigate = useNavigate();
  const setAuth = useAuthStore((s) => s.setAuth);

  useEffect(() => {
    const token = params.get("token");
    const error = params.get("error");

    if (error) {
      alert(`OAuth sign-in failed: ${error}`);
      navigate("/login");
      return;
    }
    if (!token) {
      alert("OAuth sign-in failed: missing token");
      navigate("/login");
      return;
    }

    (async () => {
      try {
        const decoded = authService.decodeUserFromToken(token);
        setAuth(decoded, token);
        try {
          const me = await authService.me();
          setAuth(
            {
              id: me.id,
              email: me.email,
              role: me.role,
              org_id: me.org_id,
              display_name: me.display_name ?? null,
              avatar_url: me.avatar_url ?? null,
            },
            token
          );
        } catch {
          // keep decoded token fallback
        }
        navigate("/");
      } catch {
        alert("OAuth token could not be processed");
        navigate("/login");
      }
    })();
  }, [navigate, params, setAuth]);

  return (
    <div className="min-h-screen bg-[#0b1022] text-white flex items-center justify-center">
      <div className="text-center">
        <p className="text-sm uppercase tracking-[0.2em] text-slate-400">TalkingBI Auth</p>
        <h1 className="text-2xl font-bold mt-2">Finalizing sign-in...</h1>
      </div>
    </div>
  );
};

export default AuthCallbackPage;
