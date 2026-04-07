export type Mode = "dashboard" | "query" | "both";

export interface UploadResponse {
  dataset_id: string;
  columns: Record<string, any>;
  row_count: number;
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
