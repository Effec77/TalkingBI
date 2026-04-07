import apiClient from "./api";

export const deleteSession = async (sessionId: string) => {
  return await apiClient.delete(`/session/${sessionId}`);
};

export const getSessionStatus = async (sessionId: string) => {
  return await apiClient.get(`/session/${sessionId}/status`);
};
