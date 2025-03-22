export const parseNumber = (str) => {
  if (!str) return null;
  const num = str.replace(/[^0-9.-]/g, "");
  return num ? parseInt(num) : null;
};

export const parsePercentage = (str) => {
  const cleanStr = str.replace(/[^\d-]/g, "");
  return Number(cleanStr) || null;
};
