import axios, { type AxiosInstance, type AxiosError, type InternalAxiosRequestConfig } from "axios";

const BASE_URL = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";

const apiClient: AxiosInstance = axios.create({
  baseURL: BASE_URL,
  timeout: 15000,
  headers: {
    "Content-Type": "application/json",
  },
});

import { useAuthStore } from "../store/useAuthStore";

const PUBLIC_PATH_PATTERNS: RegExp[] = [
  /^\/auth\/login/i,
  /^\/auth\/register/i,
  /^\/auth\/reset-password/i,
  /^\/auth\/oauth\//i,
  /^\/upload/i,
  /^\/query\//i,
  /^\/suggest/i,
  /^\/session\//i,
  /^\/metrics/i,
];

const isPublicPath = (url?: string) => {
  if (!url) return false;
  const path = url.startsWith("http")
    ? (() => {
        try {
          return new URL(url).pathname;
        } catch {
          return url;
        }
      })()
    : url;
  return PUBLIC_PATH_PATTERNS.some((pattern) => pattern.test(path));
};

// Request interceptor (auth support)
apiClient.interceptors.request.use(
  (config: InternalAxiosRequestConfig) => {
    const { token } = useAuthStore.getState();
    if (token && !isPublicPath(config.url)) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor (normalize output)
apiClient.interceptors.response.use(
  (response) => response.data,
  (error: AxiosError) => {
    if (error.response?.status === 401 && !isPublicPath(error.config?.url)) {
      useAuthStore.getState().logout();
    }
    
    if (error.response) {
      return Promise.reject({
        status: error.response.status,
        message:
          (error.response.data as any)?.message ||
          "Server error occurred",
        data: error.response.data,
      });
    }

    if (error.request) {
      return Promise.reject({
        status: 0,
        message: "No response from server",
      });
    }

    return Promise.reject({
      status: -1,
      message: error.message,
    });
  }
);

export default apiClient;
