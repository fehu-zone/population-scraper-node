import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";

const client = new Client({
  node: config.ELASTICSEARCH_HOST,
  auth: {
    username: config.ELASTIC_USERNAME,
    password: config.ELASTIC_PASSWORD,
  },
  tls: {
    rejectUnauthorized: false,
  },
});

export const initIndex = async () => {
  try {
    const indexExists = await client.indices.exists({
      index: config.INDEX_NAME,
    });

    if (!indexExists) {
      await client.indices.create({
        index: config.INDEX_NAME,
        body: {
          mappings: {
            dynamic: "strict",
            properties: {
              country: {
                type: "text",
                fields: {
                  keyword: {
                    type: "keyword",
                    ignore_above: 256,
                  },
                },
              },
              country_code: { type: "keyword" },
              continent: { type: "keyword" },
              current_population: { type: "long" },
              yearly_change: { type: "float" },
              net_change: { type: "integer" },
              migrants: { type: "integer" },
              med_age: { type: "float" },
              population_growth: { type: "float" }, // DİKKAT: "growth" yazımı kontrol et!
              "@timestamp": { type: "date" },
              is_current: { type: "boolean" },
              type: { type: "keyword" },
            },
          },
        },
      });
      console.log(`Index "${config.INDEX_NAME}" oluşturuldu.`);
    } else {
      console.log(`Index "${config.INDEX_NAME}" zaten mevcut.`);
    }

    return { created: !indexExists };
  } catch (error) {
    console.error("Index işlemleri sırasında hata:", error.message);
    throw error;
  }
};

export const updateCurrentSnapshot = async (timestamp) => {
  try {
    // Eski current'ları false yap
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      refresh: true,
      body: {
        script: {
          source: "ctx._source.is_current = false",
          lang: "painless",
        },
        query: {
          term: { is_current: true },
        },
      },
    });

    // Yeni verileri current yap
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      refresh: true,
      body: {
        script: {
          source: "ctx._source.is_current = true",
          lang: "painless",
        },
        query: {
          bool: {
            must: [
              { term: { "@timestamp": timestamp } },
              { terms: { type: ["world", "country"] } },
            ],
          },
        },
      },
    });

    console.log(`Güncel snapshot güncellendi: ${timestamp}`);
  } catch (error) {
    console.error("Snapshot güncelleme hatası:", error.message);
    throw error;
  }
};

export { client };
