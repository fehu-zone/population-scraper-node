export const parseNumber = (str) => {
  if (!str) return null;
  const numStr = String(str)
    .replace(/[^0-9-]/g, "")
    .replace(/--/g, "-");
  return numStr ? parseInt(numStr, 10) : null;
};

export const parsePercentage = (str) => {
  const cleanStr = String(str)
    .replace(/%/g, "")
    .replace(/,/g, ".")
    .replace(/[^\d.-]/g, "");

  const value = parseFloat(cleanStr);
  return !isNaN(value) ? Number(value.toFixed(2)) : null;
};
