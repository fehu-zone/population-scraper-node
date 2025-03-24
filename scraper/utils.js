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

export const bulkIndexCountries = async (countries) => {
  try {
    // Eski verileri işaretsizleştir
    await client.updateByQuery({
      index: config.INDEX_NAME,
      body: {
        script: {
          source: "ctx._source.is_current = false",
          lang: "painless",
        },
        query: {
          bool: {
            filter: [
              { term: { type: "country" } },
              { term: { is_current: true } },
            ],
          },
        },
      },
    });

    // Yeni verileri ekle
    const body = countries.flatMap((country) => [
      { index: { _index: config.INDEX_NAME } },
      country,
    ]);

    const { body: response } = await client.bulk({ refresh: true, body });

    if (response.errors) {
      console.error(
        "Bulk insert errors:",
        response.items.filter((i) => i.error)
      );
    }

    console.log(`Successfully indexed ${countries.length} countries`);
    return { success: true };
  } catch (error) {
    console.error("Country indexing error:", error.message);
    return { success: false };
  }
};
