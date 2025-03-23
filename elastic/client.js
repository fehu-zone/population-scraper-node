import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";

const client = new Client({
  node: config.ELASTICSEARCH_HOST,
  auth: {
    username: config.ELASTIC_USERNAME,
    password: config.ELASTIC_PASSWORD,
  },
  tls: { rejectUnauthorized: false },
});

export const initIndex = async () => {
  try {
    const { body: exists } = await client.indices.exists({
      index: config.INDEX_NAME,
    });

    if (exists) {
      console.log(`Index "${config.INDEX_NAME}" zaten mevcut.`);
      return { exists: true };
    }

    await client.indices.create({
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
    });
    console.log(`Index "${config.INDEX_NAME}" başarıyla oluşturuldu.`);
    return { created: true };
  } catch (error) {
    console.error(
      `Index hatası: ${error.meta?.body?.error?.reason || error.message}`
    );
    return { error };
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
