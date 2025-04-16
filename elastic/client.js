// C:\Users\Fehu\Desktop\population-scraper-node\elastic\client.js

import dotenv from "dotenv";
dotenv.config();
import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";

// .env'den veya config'den ELASTICSEARCH_HOST bilgisini alıyoruz; tanımlı değilse varsayılan olarak yerel sunucu kullanılır.
const elasticHost =
  process.env.ELASTICSEARCH_HOST ||
  config.ELASTICSEARCH_HOST ||
  "http://localhost:9200/";

// Elasticsearch istemcisi için yapılandırma nesnesi
const clientConfig = {
  node: elasticHost,
};

// Eğer kimlik doğrulama bilgileri varsa ekleyelim
if (process.env.ELASTIC_USERNAME || config.ELASTIC_USERNAME) {
  clientConfig.auth = {
    username: process.env.ELASTIC_USERNAME || config.ELASTIC_USERNAME,
    password: process.env.ELASTIC_PASSWORD || config.ELASTIC_PASSWORD,
  };
}

// Geliştirme ortamı veya özel durumlarda TLS kontrolünü kapatmak için
clientConfig.tls = { rejectUnauthorized: false };

export const client = new Client(clientConfig);

// Index oluşturma fonksiyonu
export const initIndex = async () => {
  try {
    // İndex adı .env'den veya config dosyasından alınır
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

// Snapshot güncelleme işlemini gerçekleştiren fonksiyon
export const updateCurrentSnapshot = async (timestamp) => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;

    // Önce tüm mevcut "is_current: true" kayıtlarını false yapıyoruz
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

    // Belirtilen "@timestamp" ve "type" koşullarına uyan kayıtları true yapıyoruz
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
