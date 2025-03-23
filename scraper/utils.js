export const parseNumber = (str) => {
  const numStr = String(str)
    .replace(/[^0-9-]/g, "")
    .replace(/--/g, "-")
    .trim();

  if (!numStr || numStr === "-") return null;
  const number = parseInt(numStr, 10);
  return Number.isNaN(number) ? null : number;
};

export const parsePercentage = (str) => {
  const cleanStr = String(str)
    .replace(/%/g, "")
    .replace(/,/g, ".")
    .replace(/[^\d.-]/g, "")
    .trim();

  if (!cleanStr) return null;
  const value = parseFloat(cleanStr);
  return Number.isNaN(value) ? null : Number(value.toFixed(2));
};
