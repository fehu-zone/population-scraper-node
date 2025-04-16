import dotenv from "dotenv";
dotenv.config();
import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";

const elasticHost =
  process.env.ELASTICSEARCH_HOST ||
  config.ELASTICSEARCH_HOST ||
  "http://localhost:9200/";

const clientConfig = {
  node: elasticHost,
};

if (process.env.ELASTIC_USERNAME || config.ELASTIC_USERNAME) {
  clientConfig.auth = {
    username: process.env.ELASTIC_USERNAME || config.ELASTIC_USERNAME,
    password: process.env.ELASTIC_PASSWORD || config.ELASTIC_PASSWORD,
  };
}

clientConfig.tls = { rejectUnauthorized: false };

export const client = new Client(clientConfig);

export const initIndex = async () => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;
    const { body: exists } = await client.indices.exists({ index: indexName });
    if (!exists) {
      await client.indices.create({
        index: indexName,
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
              population_growth: { type: "float" },
              // Eklenen yeni alanlar:
              births_today: { type: "long" },
              dth1s_today: { type: "long" },
              rank: { type: "integer" },
              "@timestamp": { type: "date" },
              is_current: { type: "boolean" },
              type: { type: "keyword" },
            },
          },
        },
      });
      console.log(`Index "${indexName}" oluşturuldu.`);
    } else {
      console.log(`Index "${indexName}" zaten mevcut.`);
    }
    return { created: !exists };
  } catch (error) {
    console.error("Index oluşturulamadı:", error.message);
    throw error;
  }
};

export const updateCurrentSnapshot = async (timestamp) => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;

    await client.updateByQuery({
      index: indexName,
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

    await client.updateByQuery({
      index: indexName,
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
