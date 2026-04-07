import apiClient from "./api";
import { type UploadResponse, type Mode } from "../types/api.types";

export const uploadCSV = async (
  file: File,
  mode: Mode = "both"
): Promise<UploadResponse> => {
  const formData = new FormData();
  formData.append("file", file);

  return await apiClient.post(`/upload?mode=${mode}`, formData, {
    headers: {
      "Content-Type": "multipart/form-data",
    },
  });
};
