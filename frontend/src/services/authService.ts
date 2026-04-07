import apiClient from "./api";
import { type UserCreate, type UserLogin, type TokenResponse } from "../types/api.types";
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
