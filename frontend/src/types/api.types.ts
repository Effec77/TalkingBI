export type Mode = "dashboard" | "query" | "both";

export interface UploadResponse {
  dataset_id: string;
  columns: Record<string, any>;
  row_count: number;
  profile_row_count?: number;
  profile_mode?: "full" | "sampled";
  profile: any;

  dataset_summary: any;
  dataset_summary_text: string;

  dashboard: {
    kpis: any[];
    charts: any[];
    primary_insight?: string;
    insights: string[];
  };

  suggestions: {
    type: "initial" | "followup";
    items: string[];
  };
}

export type QueryStatus =
  | "RESOLVED"
  | "INCOMPLETE"
  | "AMBIGUOUS"
  | "INVALID"
  | "MODE_BLOCKED";

export interface QueryResponse {
  status: QueryStatus;

  charts?: any[];
  data?: any;
  insights?: string[];

  suggestions?: {
    type: "followup";
    items: string[];
  };

  trace?: {
    available: boolean;
    data: any;
  };

  warnings?: string[];
  errors?: string[];
  message?: string;
}

export interface SuggestResponse {
  suggestions: string[];
}

export interface UserCreate {
  email: string;
  password: string;
}

export interface UserLogin {
  email: string;
  password: string;
}

export interface TokenResponse {
  access_token: string;
  token_type: string;
}

export interface UserProfile {
  id: string;
  email: string;
  role: string;
  org_id: string | null;
  display_name?: string | null;
  avatar_url?: string | null;
}

export interface ChangePasswordRequest {
  current_password: string;
  new_password: string;
}

export interface ProfileUpdateRequest {
  display_name?: string | null;
  avatar_url?: string | null;
}

export interface APIKeyInfo {
  id: string;
  provider: string;
  label?: string | null;
  secret_masked: string;
  created_at: string;
  updated_at: string;
}

export interface APIKeyUpsertRequest {
  provider: string;
  label?: string | null;
  secret: string;
}

export interface ActivityInfo {
  id: string;
  event_type: string;
  provider?: string | null;
  ip_address?: string | null;
  user_agent?: string | null;
  created_at: string;
}
