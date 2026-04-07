export const logEvent = (event: string, data?: any) => {
  console.log(`[LOG] ${event}`, data || '');
};
