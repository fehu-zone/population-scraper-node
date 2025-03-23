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
  COUNTRY_URL: process.env.COUNTRY_URL,
  WORLD_URL: process.env.WORLD_URL,
};

// ================= ELASTICSEARCH CLIENT =================
const client = new Client({
  node: config.ELASTICSEARCH_HOST,
  auth: {
    username: config.ELASTIC_USERNAME,
    password: config.ELASTIC_PASSWORD,
  },
  tls: { rejectUnauthorized: false },
});

// ================= UTILITIES =================
const parseNumber = (str) => {
  if (!str) return null;
  const num = str.replace(/[^\d-]/g, "");
  return num.length ? parseInt(num, 10) : null;
};

const parsePercentage = (str) => {
  const cleanStr = str.replace(/%/g, "").replace(/,/g, ".");
  return parseFloat(cleanStr) || null;
};

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

// ... (importlar aynı)

// ================= ELASTICSEARCH OPERATIONS =================
const initIndex = async () => {
  try {
    const { body: exists } = await client.indices.exists({
      index: config.INDEX_NAME,
    });

    if (exists) {
      log("ℹ️", "cyan", `Index "${config.INDEX_NAME}" zaten mevcut`);
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
    log("✅", "green", `Index "${config.INDEX_NAME}" oluşturuldu`);
    return { created: true };
  } catch (error) {
    log(
      "❌",
      "red",
      `Index hatası: ${error.meta?.body?.error?.reason || error.message}`
    );
    return { error };
  }
};

// ================= DATA SCRAPING =================
const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(config.COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 20000,
    });

    const $ = cheerio.load(data);
    return $("#example2 tbody tr")
      .map((i, row) => {
        const cells = $(row).find("td");
        if (cells.length < 10) return null;

        const countryName = $(cells[1]).text().trim();
        return {
          country: COUNTRY_MAPPING[countryName] || countryName,
          current_population: parseNumber($(cells[2]).text()),
          yearly_change: parsePercentage($(cells[3]).text()),
          net_change: parseNumber($(cells[4]).text()),
          migrants: parseNumber($(cells[7]).text()),
          med_age: parseNumber($(cells[9]).text()),
        };
      })
      .get()
      .filter((item) => item?.current_population > 0);
  } catch (error) {
    log("❌", "red", `Ülke veri hatası: ${error.message}`);
    return [];
  }
};

// ================= MAIN PROCESS =================
const validateData = (worldData, countryData) => {
  const errors = [];

  if (!worldData?.current_population) {
    errors.push("Dünya nüfus verisi eksik");
  }

  if (!countryData?.length || countryData.length < 100) {
    errors.push(`Yetersiz ülke verisi: ${countryData?.length || 0} kayıt`);
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
};

const processData = async () => {
  try {
    log("🚀", "cyan", "\nScraping başlıyor...\n====================");

    // 1. Index yönetimi
    const indexResult = await initIndex();
    if (indexResult.error) throw indexResult.error;

    // 2. Paralel veri çekme
    const [worldData, countryData] = await Promise.all([
      fetchWorldData(),
      fetchCountryData(),
    ]);

    // 3. Detaylı validasyon
    const validation = validateData(worldData, countryData);
    if (!validation.isValid) {
      throw new Error(`Validasyon hatası: ${validation.errors.join(", ")}`);
    }

    // 4. Elasticsearch'e gönder
    const timestamp = new Date().toISOString();
    const bulkBody = [];

    // Dünya verisi
    bulkBody.push(
      { index: { _index: config.INDEX_NAME, _id: `world_${timestamp}` } },
      {
        ...worldData,
        type: "world",
        is_current: true,
        "@timestamp": timestamp,
      }
    );

    // Ülke verileri
    countryData.forEach((country) => {
      bulkBody.push(
        {
          index: {
            _index: config.INDEX_NAME,
            _id: `country_${country.country}_${timestamp}`,
          },
        },
        {
          ...country,
          type: "country",
          is_current: true,
          "@timestamp": timestamp,
        }
      );
    });

    // 5. Toplu ekleme
    const { body: bulkResponse } = await client.bulk({
      refresh: "wait_for",
      body: bulkBody,
    });

    // 6. Hata yönetimi
    if (bulkResponse.errors) {
      const failedDocs = bulkResponse.items
        .filter((item) => item.index.error)
        .map((item) => item.index._id);

      log("❌", "red", `Hatalı dokümanlar: ${failedDocs.length}`);
      console.error("Hata detayları:", failedDocs.slice(0, 3));
    }

    // 7. Snapshot güncelleme
    await markCurrentSnapshot(timestamp);

    log(
      "🎉",
      "green",
      `İşlem tamamlandı! ${bulkResponse.items.length} doküman eklendi`
    );
  } catch (error) {
    log("💀", "red", `Kritik hata: ${error.message}`);
    setTimeout(processData, 300000); // 5 dakika sonra tekrar dene
  }
};

console.clear();
processData();
setInterval(processData, 1800000); // 30 dakikada bir çalıştır
