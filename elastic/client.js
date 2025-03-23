import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";

// client.js içinde
const client = new Client({
  node: config.ELASTICSEARCH_HOST,
  auth: {
    username: config.ELASTIC_USERNAME,
    password: config.ELASTIC_PASSWORD,
  },
  tls: {
    rejectUnauthorized: false, // Sertifika doğrulamayı devre dışı bırak
  },
});

export const initIndex = async () => {
  try {
    // 1. Adım: İndex metadata'sını temizle
    await client.indices.delete({
      index: config.INDEX_NAME,
      ignore_unavailable: true,
    });

    // 2. Adım: Yeniden oluştur
    await client.indices.create(
      {
        index: config.INDEX_NAME,
        body: {
          mappings: {
            dynamic: "strict",
            properties: {
              country: { type: "keyword" },
              current_population: { type: "long" },
              yearly_change: { type: "scaled_float", scaling_factor: 100 },
              net_change: { type: "integer" },
              migrants: { type: "integer" },
              med_age: { type: "float" },
              population_growth: { type: "integer" },
              "@timestamp": { type: "date" },
              is_current: { type: "boolean" },
              type: { type: "keyword" },
            },
          },
        },
      },
      { ignore: [400, 404] }
    ); // 400 (Bad Request) ve 404 (Not Found) hatalarını yoksay

    console.log(`Index "${config.INDEX_NAME}" başarıyla resetlendi.`);
    return { created: true };
  } catch (error) {
    if (error.meta?.body?.error?.type === "resource_already_exists_exception") {
      console.log(`Index "${config.INDEX_NAME}" zaten aktif.`);
      return { exists: true };
    }
    console.error(
      `Kritik hata: ${error.meta?.body?.error?.reason || error.message}`
    );
    throw error;
  }
};

export const markCurrentSnapshot = async (timestamp) => {
  try {
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      body: {
        script: {
          source: "ctx._source.is_current = false",
          lang: "painless",
        },
        query: {
          bool: {
            filter: [{ term: { is_current: true } }],
          },
        },
      },
    });

    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      body: {
        script: {
          source: "ctx._source.is_current = true",
          lang: "painless",
        },
        query: {
          bool: {
            must: [
              { term: { "@timestamp": timestamp } },
              { term: { type: "world" } },
            ],
          },
        },
      },
    });

    console.log(`Snapshot güncellendi: ${timestamp}`);
  } catch (error) {
    console.error("Snapshot güncelleme hatası:", error.message);
  }
};

export { client };
