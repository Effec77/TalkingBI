import React, { useEffect, useState } from "react";

import Layout from "../components/Common/Layout";
import { authService } from "../services/authService";
import { useAuthStore } from "../store/useAuthStore";
import type { APIKeyInfo, ActivityInfo } from "../types/api.types";

type Preferences = {
  defaultMode: "both" | "dashboard" | "query";
  denseTables: boolean;
  smartSuggestions: boolean;
};

const PREF_KEY = "talkingbi_user_preferences_v1";

const SettingsPage: React.FC = () => {
  const user = useAuthStore((s) => s.user);
  const setUser = useAuthStore((s) => s.setUser);

  const [displayName, setDisplayName] = useState("");
  const [avatarUrl, setAvatarUrl] = useState("");
  const [provider, setProvider] = useState("openai");
  const [apiLabel, setApiLabel] = useState("");
  const [apiSecret, setApiSecret] = useState("");
  const [apiKeys, setApiKeys] = useState<APIKeyInfo[]>([]);
  const [activity, setActivity] = useState<ActivityInfo[]>([]);
  const [status, setStatus] = useState("");
  const [saving, setSaving] = useState(false);
  const [savingPassword, setSavingPassword] = useState(false);

  const [preferences, setPreferences] = useState<Preferences>({
    defaultMode: "both",
    denseTables: false,
    smartSuggestions: true,
  });
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");

  const loadRemote = async () => {
    const [me, keys, logs] = await Promise.all([
      authService.me(),
      authService.listApiKeys(),
      authService.activity(20),
    ]);
    setUser({
      id: me.id,
      email: me.email,
      role: me.role,
      org_id: me.org_id,
      display_name: me.display_name ?? null,
      avatar_url: me.avatar_url ?? null,
    });
    setDisplayName(me.display_name || "");
    setAvatarUrl(me.avatar_url || "");
    setApiKeys(keys);
    setActivity(logs);
  };

  useEffect(() => {
    try {
      const raw = localStorage.getItem(PREF_KEY);
      if (raw) setPreferences(JSON.parse(raw));
    } catch {
      // ignore malformed local settings
    }
    loadRemote().catch(() => {});
  }, []);

  const savePreferences = () => {
    localStorage.setItem(PREF_KEY, JSON.stringify(preferences));
    setStatus("Preferences saved.");
  };

  const saveProfile = async () => {
    setSaving(true);
    setStatus("");
    try {
      const me = await authService.updateProfile({
        display_name: displayName || null,
        avatar_url: avatarUrl || null,
      });
      setUser({
        id: me.id,
        email: me.email,
        role: me.role,
        org_id: me.org_id,
        display_name: me.display_name ?? null,
        avatar_url: me.avatar_url ?? null,
      });
      setStatus("Profile updated.");
    } catch (err: any) {
      setStatus(String(err?.message || "Failed to update profile"));
    } finally {
      setSaving(false);
    }
  };

  const updatePassword = async (e: React.FormEvent) => {
    e.preventDefault();
    setSavingPassword(true);
    try {
      await authService.changePassword({ current_password: currentPassword, new_password: newPassword });
      setCurrentPassword("");
      setNewPassword("");
      setStatus("Password updated.");
    } catch (err: any) {
      setStatus(String(err?.message || "Password update failed"));
    } finally {
      setSavingPassword(false);
    }
  };

  const saveApiKey = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!apiSecret.trim()) {
      setStatus("API secret is required.");
      return;
    }
    try {
      await authService.upsertApiKey({
        provider,
        label: apiLabel || provider,
        secret: apiSecret,
      });
      setApiSecret("");
      setApiLabel("");
      setStatus("API key saved.");
      await loadRemote();
    } catch (err: any) {
      setStatus(String(err?.message || "Failed to save API key"));
    }
  };

  const removeApiKey = async (id: string) => {
    try {
      await authService.deleteApiKey(id);
      setStatus("API key removed.");
      await loadRemote();
    } catch (err: any) {
      setStatus(String(err?.message || "Failed to remove API key"));
    }
  };

  return (
    <Layout>
      <div className="space-y-6">
        <header className="rounded-xl bg-[#1e3a72] text-white px-6 py-5 border border-[#2e4f8d]">
          <p className="text-xs uppercase tracking-[0.22em] text-blue-100/90">User Settings</p>
          <h1 className="text-3xl font-bold mt-2">Account, Security, and Provider Access</h1>
        </header>

        {status ? (
          <div className="rounded-lg border border-[#cfd8ea] bg-white px-4 py-3 text-sm text-[#17325f]">{status}</div>
        ) : null}

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
          <section className="xl:col-span-2 space-y-5">
            <div className="rounded-xl border border-[#cfd8ea] bg-white p-6 space-y-4">
              <h2 className="text-sm uppercase tracking-[0.2em] text-[#2f4b7f]">Profile</h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3 text-sm text-[#17325f]">
                <input
                  value={displayName}
                  onChange={(e) => setDisplayName(e.target.value)}
                  placeholder="Display name"
                  className="h-10 rounded-lg border border-[#d4dfef] px-3"
                />
                <input value={user?.email || ""} disabled className="h-10 rounded-lg border border-[#d4dfef] px-3 bg-slate-50" />
                <input
                  value={avatarUrl}
                  onChange={(e) => setAvatarUrl(e.target.value)}
                  placeholder="Avatar URL (optional)"
                  className="md:col-span-2 h-10 rounded-lg border border-[#d4dfef] px-3"
                />
              </div>
              <button
                onClick={saveProfile}
                disabled={saving}
                className="h-10 px-4 rounded-lg bg-[#1f4b9a] text-white font-semibold disabled:opacity-60"
              >
                {saving ? "Saving..." : "Save Profile"}
              </button>
            </div>

            <div className="rounded-xl border border-[#cfd8ea] bg-white p-6">
              <h2 className="text-sm uppercase tracking-[0.2em] text-[#2f4b7f] mb-4">Preferences</h2>
              <div className="space-y-4">
                <div>
                  <label className="text-xs uppercase tracking-widest text-slate-500">Default Upload Mode</label>
                  <select
                    className="mt-2 w-full h-11 rounded-lg border border-[#d4dfef] px-3 text-[#17325f]"
                    value={preferences.defaultMode}
                    onChange={(e) => setPreferences((p) => ({ ...p, defaultMode: e.target.value as Preferences["defaultMode"] }))}
                  >
                    <option value="both">Dashboard + Query</option>
                    <option value="dashboard">Dashboard Only</option>
                    <option value="query">Query Only</option>
                  </select>
                </div>
                <label className="flex items-center justify-between text-sm text-[#17325f]">
                  <span>Dense data tables</span>
                  <input
                    type="checkbox"
                    checked={preferences.denseTables}
                    onChange={(e) => setPreferences((p) => ({ ...p, denseTables: e.target.checked }))}
                  />
                </label>
                <label className="flex items-center justify-between text-sm text-[#17325f]">
                  <span>Smart query suggestions</span>
                  <input
                    type="checkbox"
                    checked={preferences.smartSuggestions}
                    onChange={(e) => setPreferences((p) => ({ ...p, smartSuggestions: e.target.checked }))}
                  />
                </label>
                <button onClick={savePreferences} className="h-11 px-5 rounded-lg bg-[#1f4b9a] text-white font-semibold">
                  Save Preferences
                </button>
              </div>
            </div>
          </section>

          <aside className="space-y-5">
            <div className="rounded-xl border border-[#cfd8ea] bg-white p-6">
              <h2 className="text-sm uppercase tracking-[0.2em] text-[#2f4b7f] mb-4">Security</h2>
              <form className="space-y-3" onSubmit={updatePassword}>
                <input
                  type="password"
                  value={currentPassword}
                  onChange={(e) => setCurrentPassword(e.target.value)}
                  placeholder="Current password"
                  className="w-full h-10 rounded-lg border border-[#d4dfef] px-3 text-[#17325f]"
                  required
                />
                <input
                  type="password"
                  value={newPassword}
                  onChange={(e) => setNewPassword(e.target.value)}
                  placeholder="New password"
                  className="w-full h-10 rounded-lg border border-[#d4dfef] px-3 text-[#17325f]"
                  required
                />
                <button type="submit" disabled={savingPassword} className="w-full h-10 rounded-lg bg-[#17325f] text-white font-semibold disabled:opacity-50">
                  {savingPassword ? "Updating..." : "Update Password"}
                </button>
              </form>
            </div>

            <div className="rounded-xl border border-[#cfd8ea] bg-white p-6 space-y-3">
              <h2 className="text-sm uppercase tracking-[0.2em] text-[#2f4b7f]">Provider API Keys</h2>
              <form className="space-y-2" onSubmit={saveApiKey}>
                <input value={provider} onChange={(e) => setProvider(e.target.value)} placeholder="Provider (groq/openai/anthropic)" className="h-9 w-full rounded border border-[#d4dfef] px-2 text-sm" />
                <input value={apiLabel} onChange={(e) => setApiLabel(e.target.value)} placeholder="Label (optional)" className="h-9 w-full rounded border border-[#d4dfef] px-2 text-sm" />
                <input type="password" value={apiSecret} onChange={(e) => setApiSecret(e.target.value)} placeholder="Secret" className="h-9 w-full rounded border border-[#d4dfef] px-2 text-sm" />
                <button type="submit" className="h-9 px-3 rounded bg-[#1f4b9a] text-white text-sm font-semibold">Save Key</button>
              </form>
              <div className="space-y-2">
                {apiKeys.map((k) => (
                  <div key={k.id} className="flex items-center justify-between text-xs border border-[#d4dfef] rounded px-2 py-1.5">
                    <span>{k.provider} ({k.secret_masked})</span>
                    <button onClick={() => removeApiKey(k.id)} className="text-red-600">Delete</button>
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-xl border border-[#cfd8ea] bg-white p-6">
              <h2 className="text-sm uppercase tracking-[0.2em] text-[#2f4b7f] mb-3">Recent Auth Activity</h2>
              <ul className="text-xs text-[#17325f] space-y-2 max-h-48 overflow-auto">
                {activity.map((a) => (
                  <li key={a.id} className="border-b border-slate-100 pb-2">
                    <span className="font-semibold">{a.event_type}</span>
                    {a.provider ? ` (${a.provider})` : ""} - {new Date(a.created_at).toLocaleString()}
                  </li>
                ))}
              </ul>
            </div>
          </aside>
        </div>
      </div>
    </Layout>
  );
};

export default SettingsPage;
