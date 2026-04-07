import apiClient from "./api";
import {
  type UserCreate,
  type UserLogin,
  type TokenResponse,
  type UserProfile,
  type ChangePasswordRequest,
  type ProfileUpdateRequest,
  type APIKeyInfo,
  type APIKeyUpsertRequest,
  type ActivityInfo,
} from "../types/api.types";
import { jwtDecode } from "jwt-decode";

interface DecodedToken {
  user_id: string;
  exp: number;
}

export const authService = {
  async register(data: UserCreate): Promise<TokenResponse> {
    const response: TokenResponse = await apiClient.post("/auth/register", data);
    return response;
  },

  async login(data: UserLogin): Promise<TokenResponse> {
    const response: TokenResponse = await apiClient.post("/auth/login", data);
    return response;
  },

  async me(): Promise<UserProfile> {
    return await apiClient.get("/auth/me");
  },

  async changePassword(data: ChangePasswordRequest): Promise<{ status: string; message: string }> {
    return await apiClient.post("/auth/change-password", data);
  },

  async updateProfile(data: ProfileUpdateRequest): Promise<UserProfile> {
    return await apiClient.put("/auth/profile", data);
  },

  async listApiKeys(): Promise<APIKeyInfo[]> {
    return await apiClient.get("/auth/api-keys");
  },

  async upsertApiKey(data: APIKeyUpsertRequest): Promise<APIKeyInfo> {
    return await apiClient.post("/auth/api-keys", data);
  },

  async deleteApiKey(id: string): Promise<{ status: string }> {
    return await apiClient.delete(`/auth/api-keys/${id}`);
  },

  async activity(limit = 20): Promise<ActivityInfo[]> {
    return await apiClient.get(`/auth/activity?limit=${limit}`);
  },

  startOAuth(provider: "google" | "github") {
    const apiBase = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";
    const callback = `${window.location.origin}/auth/callback`;
    const url = `${apiBase}/auth/oauth/${provider}/start?redirect_uri=${encodeURIComponent(callback)}`;
    window.location.href = url;
  },

  decodeUserFromToken(token: string) {
    const decoded = jwtDecode<DecodedToken>(token);
    return {
      id: decoded.user_id,
      email: "", // Token only contains user_id for now, could add more
      role: "user",
      org_id: null,
    };
  }
};
