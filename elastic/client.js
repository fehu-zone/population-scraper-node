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
    const exists = await client.indices.exists({ index: config.INDEX_NAME });
    if (exists.body) {
      // ES 7.x+ için doğru kontrol
      console.log(`Index "${config.INDEX_NAME}" zaten mevcut.`);
      return;
    }
    // İndeks oluşturulurken örnek mapping tanımı eklenmiştir. Gereksinimlerinize göre düzenleyebilirsiniz.
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
  } catch (error) {
    console.error("Index oluşturulurken hata:", error.message);
  }
};

export const markCurrentSnapshot = async (timestamp) => {
  try {
    // Önce tüm dökümanların is_current alanını false yapıyoruz
    await client.updateByQuery({
      index: config.INDEX_NAME,
      body: {
        script: { source: "ctx._source.is_current = false", lang: "painless" },
        query: { match_all: {} },
      },
    });

    // Belirtilen timestamp'e sahip dökümanın is_current alanını true yapıyoruz
    await client.updateByQuery({
      index: config.INDEX_NAME,
      body: {
        script: { source: "ctx._source.is_current = true", lang: "painless" },
        query: { term: { "@timestamp": timestamp } },
      },
    });
    console.log(`Snapshot güncellendi: ${timestamp}`);
  } catch (error) {
    console.error("Snapshot güncellenirken hata:", error.message);
  }
};

export { client };
