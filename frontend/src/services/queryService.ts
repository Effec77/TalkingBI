import apiClient from "./api";
import { type QueryResponse } from "../types/api.types";

export const runQuery = async (
  sessionId: string,
  query: string
): Promise<QueryResponse> => {
  return await apiClient.post(`/query/${sessionId}`, {
    query,
  });
};
