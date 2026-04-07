import apiClient from "./api";

export const getSuggestions = async (
  sessionId: string,
  prefix: string
): Promise<{ suggestions: string[] }> => {
  return await apiClient.get(`/suggest?session_id=${sessionId}&q=${prefix}`);
};
