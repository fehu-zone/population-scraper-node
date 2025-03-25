import { client } from "../elastic/client.js";
import config from "../config/index.js";

export const parseNumber = (str, isPercentage = false) => {
  if ([null, undefined, ""].includes(str)) return 0; // Null değerler için 0

  const cleaned = String(str)
    .replace(/[^\d.-]/g, "")
    .replace(/^\-/g, "-");

  const number = parseFloat(cleaned);
  return Number.isNaN(number) ? 0 : number; // NaN durumunda 0
};

export const cleanCountryName = (name) => {
  const COUNTRY_NAME_MAPPING = {
    /* ... */
  };

  return (
    COUNTRY_NAME_MAPPING[name] ||
    name
      .replace(/\[.*?\]/g, "")
      .replace(/\(.*?\)/g, "")
      .trim()
  );
};

export const bulkIndexCountries = async (countries) => {
  try {
    // EKSİK PARANTEZ DÜZELTİLDİ
    if (!Array.isArray(countries)) {
      throw new Error("Geçersiz ülke veri formatı");
    }

    const { body: updateResponse } = await client.updateByQuery({
      /* ... */
    });

    const body = countries.flatMap((country) => [
      /* ... */
    ]);

    const { body: bulkResponse } = await client.bulk({
      /* ... */
    });

    return {
      /* ... */
    };
  } catch (error) {
    return {
      /* ... */
    };
  }
};
