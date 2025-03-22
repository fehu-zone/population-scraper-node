import dotenv from "dotenv";
import axios from "axios";
import * as cheerio from "cheerio";
import { Client } from "@elastic/elasticsearch";

dotenv.config();

// ================= CONFIG =================
const config = {
  ELASTICSEARCH_HOST: process.env.ELASTICSEARCH_HOST,
  INDEX_NAME: process.env.INDEX_NAME,
  ELASTIC_USERNAME: process.env.ELASTIC_USERNAME,
  ELASTIC_PASSWORD: process.env.ELASTIC_PASSWORD,
  REQUEST_HEADERS: {
    "User-Agent": process.env.USER_AGENT,
  },
};

// ================= ELASTICSEARCH CLIENT & FUNCTIONS =================
const client = new Client({
  node: config.ELASTICSEARCH_HOST,
  auth: {
    username: config.ELASTIC_USERNAME,
    password: config.ELASTIC_PASSWORD,
  },
  tls: { rejectUnauthorized: false },
});

const initIndex = async () => {
  try {
    const { body: exists } = await client.indices.exists({
      index: config.INDEX_NAME,
    });
    if (exists) {
      console.log(`Index "${config.INDEX_NAME}" zaten mevcut.`);
      return;
    }
    await client.indices.create({
      index: config.INDEX_NAME,
      body: {
        mappings: {
          properties: {
            country: { type: "keyword" },
            current_population: { type: "integer" },
            yearly_change: { type: "float" },
            net_change: { type: "integer" },
            migrants: { type: "integer" },
            med_age: { type: "float" },
            "@timestamp": { type: "date" },
            is_current: { type: "boolean" },
          },
        },
      },
    });
    console.log(`Index "${config.INDEX_NAME}" başarıyla oluşturuldu.`);
  } catch (error) {
    if (error.meta?.body?.error?.type === "resource_already_exists_exception") {
      console.log(`Index "${config.INDEX_NAME}" zaten mevcut (hata atlandı).`);
    } else {
      console.error("Index oluşturulurken hata:", error.message);
    }
  }
};

const markCurrentSnapshot = async (timestamp) => {
  try {
    // Tüm dökümanların is_current alanını false yaparken conflicts: "proceed" ekleniyor.
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      body: {
        script: { source: "ctx._source.is_current = false", lang: "painless" },
        query: { match_all: {} },
      },
    });
    // Belirtilen timestamp'e sahip dökümanın is_current alanını true yapıyoruz.
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
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

// ================= UTILS =================
const parseNumber = (str) => {
  if (!str) return null;
  const num = str.replace(/[^0-9.-]/g, "");
  return num ? parseInt(num) : null;
};

const parsePercentage = (str) => {
  const cleanStr = str.replace(/[^\d-]/g, "");
  return Number(cleanStr) || null;
};

// ================= SCRAPER FUNCTIONS =================
// Ülke isimleri eşleştirmesi (örnek eşleşmeler)
const COUNTRY_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  // Diğer eşleşmeleri ekleyin...
};

const COUNTRY_URL =
  "https://www.worldometers.info/world-population/population-by-country";

const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      params: { order: "desc", orderby: "population" },
    });
    const $ = cheerio.load(data);
    const rows = $("#example2 tbody tr");

    const countries = rows
      .map((i, row) => {
        const cells = $(row).find("td");
        return {
          country: COUNTRY_MAPPING[cells.eq(1).text()] || cells.eq(1).text(),
          current_population: parseNumber(cells.eq(2).text()),
          yearly_change: parsePercentage(cells.eq(3).text()),
          net_change: parseNumber(cells.eq(4).text()),
          migrants: parseNumber(cells.eq(7).text()),
          med_age: parseNumber(cells.eq(9).text()),
        };
      })
      .get();

    // Eğer veri elemanlarına "@timestamp" ekli değilse, daha sonra bulk oluştururken ekleyebiliriz.
    return countries;
  } catch (error) {
    console.error("Ülke verileri çekilirken hata:", error.message);
    return [];
  }
};

const WORLD_URL = "https://www.worldometers.info/world-population/";

const fetchWorldData = async () => {
  try {
    const { data } = await axios.get(WORLD_URL, {
      headers: config.REQUEST_HEADERS,
    });
    const $ = cheerio.load(data);

    const currentPopulation = parseNumber(
      $(".rts-counter span").first().text()
    );
    const birthsToday = parseNumber(
      $('div:contains("Births today")')
        .closest(".col-md-3")
        .find(".rts-counter")
        .text()
    );
    const deathsToday = parseNumber(
      $('div:contains("Deaths today")')
        .closest(".col-md-3")
        .find(".rts-counter")
        .text()
    );

    return {
      current_population: currentPopulation,
      births_today: birthsToday,
      deaths_today: deathsToday,
    };
  } catch (error) {
    console.error("Dünya verileri çekilirken hata:", error.message);
    return null;
  }
};

// ================= LOG & PROCESS =================
const colors = {
  cyan: "\x1b[36m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  reset: "\x1b[0m",
};

const log = (emoji, color, message) => {
  const timestamp = new Date().toLocaleTimeString();
  console.log(
    `${colors[color]}${emoji} [${timestamp}] ${message}${colors.reset}`
  );
};

const PROCESS_INTERVAL = 300_000; // 5 dakika

const processData = async () => {
  try {
    log(
      "🚀",
      "cyan",
      "\nKazıma İşlemi Başlatılıyor...\n==============================="
    );

    log("🔧", "cyan", "Elasticsearch index kontrol ediliyor...");
    await initIndex();

    const timestamp = new Date().toISOString();

    log("🌍", "yellow", "Dünya verileri çekiliyor...");
    const world = await fetchWorldData();
    if (!world || !world.current_population) {
      console.error(
        "Dünya verileri çekilemedi veya eksik. İşlem durduruluyor."
      );
      return; // Null veya eksik veri varsa işlemi atla.
    }
    // Eğer dünya verisine zaman damgası eklenmemişse, ekliyoruz.
    if (!world["@timestamp"]) {
      world["@timestamp"] = timestamp;
    }
    log(
      "✅",
      "green",
      `Dünya Verileri Alındı:\nNüfus: ${world.current_population}\nDoğum: ${world.births_today}\nÖlüm: ${world.deaths_today}`
    );

    log("🌐", "yellow", "Ülke verileri çekiliyor...");
    const countries = await fetchCountryData();
    const successfulCountries = countries.filter((c) => c.current_population);
    const failedCountries = countries.filter((c) => !c.current_population);

    if (failedCountries.length > 0) {
      log(
        "❌",
        "red",
        `Hatalı Ülkeler: ${failedCountries.map((c) => c.country).join(", ")}`
      );
    }
    log("✅", "green", `Başarılı Ülke Sayısı: ${successfulCountries.length}`);

    log(
      "📤",
      "cyan",
      "\nVeriler Elasticsearch'e Gönderiliyor...\n---------------------------------------"
    );

    // Her veri için zaman damgası yoksa ekliyoruz
    const countriesWithTimestamp = successfulCountries.map((country) => ({
      ...country,
      "@timestamp": country["@timestamp"] || timestamp,
    }));

    const bulkBody = [
      { index: { _index: config.INDEX_NAME } },
      {
        "@timestamp": world["@timestamp"],
        type: "world",
        is_current: true,
        world,
      },
      ...countriesWithTimestamp.flatMap((country) => [
        { index: { _index: config.INDEX_NAME } },
        {
          "@timestamp": country["@timestamp"],
          type: "country",
          is_current: true,
          ...country,
        },
      ]),
    ];

    try {
      const { body: bulkResponse } = await client.bulk({
        body: bulkBody,
        refresh: true,
      });
      if (!bulkResponse) {
        throw new Error("Bulk yanıtı alınamadı");
      }
      if (bulkResponse.errors) {
        const errors = bulkResponse.items.filter(
          (item) => item.index && item.index.error
        );
        log("❌", "red", `Hatalı Gönderimler: ${errors.length}`);
      } else {
        log(
          "✅",
          "green",
          `Başarıyla Gönderilen Toplam Kayıt: ${bulkResponse.items.length}`
        );
      }
    } catch (bulkError) {
      log("💀", "red", `Bulk işlem hatası: ${bulkError.message}`);
    }

    log("🔁", "cyan", "\nMevcut Snapshot Güncelleniyor...");
    await markCurrentSnapshot(timestamp);

    log(
      "🎉",
      "green",
      "\nİşlem Başarıyla Tamamlandı!\n==========================="
    );
    console.log("\n");
  } catch (error) {
    log("💀", "red", `KRİTİK HATA: ${error.message}`);
  }
};

// ================= PROCESS RUNNER =================
// setInterval kullanarak her 5 dakikada bir processData çalıştırıyoruz.
console.clear();
processData(); // Başlangıçta çalıştır
setInterval(processData, PROCESS_INTERVAL);
