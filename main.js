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
  COUNTRY_URL:
    "https://www.worldometers.info/world-population/population-by-country",
  WORLD_URL: "https://www.worldometers.info/world-population/",
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

// ================= ELASTICSEARCH OPERATIONS =================
const initIndex = async () => {
  try {
    const { statusCode } = await client.indices.exists({
      index: config.INDEX_NAME,
    });

    if (statusCode === 200) {
      log("ℹ️", "cyan", `Index "${config.INDEX_NAME}" already exists`);
      return;
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
            "@timestamp": { type: "date" },
            is_current: { type: "boolean" },
            type: { type: "keyword" },
          },
        },
      },
    });
    log("✅", "green", `Index "${config.INDEX_NAME}" created successfully`);
  } catch (error) {
    log(
      "❌",
      "red",
      `Index creation error: ${
        error.meta?.body?.error?.reason || error.message
      }`
    );
  }
};

const markCurrentSnapshot = async (timestamp) => {
  try {
    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      body: {
        script: {
          source: "ctx._source.is_current = params.new_status",
          lang: "painless",
          params: { new_status: false },
        },
        query: { match_all: {} },
      },
    });

    await client.updateByQuery({
      index: config.INDEX_NAME,
      conflicts: "proceed",
      body: {
        script: {
          source: "ctx._source.is_current = params.new_status",
          lang: "painless",
          params: { new_status: true },
        },
        query: { term: { "@timestamp": timestamp } },
      },
    });
    log("🔄", "cyan", `Snapshot updated for ${timestamp}`);
  } catch (error) {
    log("❌", "red", `Snapshot update error: ${error.message}`);
  }
};

// ================= DATA SCRAPING =================
const COUNTRY_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  Iran: "Iran (Islamic Republic of)",
  Vietnam: "Viet Nam",
  "South Korea": "Republic of Korea",
};

const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(config.COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 15000,
    });

    const $ = cheerio.load(data);
    const rows = $("#example2 tbody tr");

    return rows
      .map((i, row) => {
        const cells = $(row).find("td");
        return {
          country:
            COUNTRY_MAPPING[$(cells[1]).text().trim()] ||
            $(cells[1]).text().trim(),
          current_population: parseNumber($(cells[2]).text()),
          yearly_change: parsePercentage($(cells[3]).text()),
          net_change: parseNumber($(cells[4]).text()),
          migrants: parseNumber($(cells[7]).text()),
          med_age: parseNumber($(cells[9]).text()),
        };
      })
      .get()
      .filter((item) => item.current_population);
  } catch (error) {
    log("❌", "red", `Country data error: ${error.message}`);
    return [];
  }
};

const fetchWorldData = async () => {
  try {
    const { data } = await axios.get(config.WORLD_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 15000,
    });

    const $ = cheerio.load(data);
    const mainCounter = parseNumber(
      $(".rts-counter").first().text().replace(/,/g, "")
    );
    const counters = $(".counter-item")
      .slice(0, 2)
      .map((i, el) =>
        parseNumber($(el).find(".counter-value").text().replace(/,/g, ""))
      )
      .get();

    return {
      current_population: mainCounter,
      births_today: counters[0],
      deaths_today: counters[1],
      "@timestamp": new Date().toISOString(),
    };
  } catch (error) {
    log("❌", "red", `World data error: ${error.message}`);
    return null;
  }
};

// ================= MAIN PROCESS =================
const validateData = (worldData, countryData) => {
  const errors = [];

  if (!worldData?.current_population)
    errors.push("Invalid world population data");
  if (!countryData?.length) errors.push("No country data available");
  if (worldData && typeof worldData.current_population !== "number") {
    errors.push("Invalid world population format");
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
};

const processData = async () => {
  try {
    log(
      "🚀",
      "cyan",
      "\nStarting scraping process...\n========================="
    );

    // Index initialization
    log("🔧", "cyan", "Checking Elasticsearch index...");
    await initIndex();

    // Data collection
    log("🌍", "yellow", "Fetching world data...");
    const worldData = await fetchWorldData();

    log("🌐", "yellow", "Fetching country data...");
    const countryData = await fetchCountryData();

    // Data validation
    const validation = validateData(worldData, countryData);
    if (!validation.isValid) {
      log("❌", "red", `Validation failed: ${validation.errors.join(", ")}`);
      return;
    }

    // Prepare documents
    const timestamp = new Date().toISOString();
    const bulkBody = [];

    // Add world data
    bulkBody.push(
      { index: { _index: config.INDEX_NAME, _id: `world_${timestamp}` } },
      {
        ...worldData,
        "@timestamp": timestamp,
        type: "world",
        is_current: true,
      }
    );

    // Add country data
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
          "@timestamp": timestamp,
          type: "country",
          is_current: true,
        }
      );
    });

    // Data ingestion
    log(
      "📤",
      "cyan",
      "\nSending data to Elasticsearch...\n-----------------------------"
    );
    const { body: bulkResponse } = await client.bulk({
      refresh: "wait_for",
      body: bulkBody,
    });

    if (bulkResponse.errors) {
      const errorCount = bulkResponse.items.filter(
        (item) => item.index.error
      ).length;
      log("❌", "red", `Failed documents: ${errorCount}`);
    } else {
      log(
        "✅",
        "green",
        `Successfully ingested ${bulkResponse.items.length} documents`
      );
    }

    // Update current snapshot
    log("🔁", "cyan", "\nUpdating current snapshot...");
    await markCurrentSnapshot(timestamp);

    log(
      "🎉",
      "green",
      "\nProcess completed successfully!\n=============================="
    );
  } catch (error) {
    log("💀", "red", `Critical error: ${error.message}`);
    setTimeout(processData, 5000); // Retry after 5 seconds on critical errors
  }
};

// ================= INITIALIZATION =================
console.clear();
processData();
setInterval(processData, 300000); // 5 minutes interval
