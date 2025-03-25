import dotenv from "dotenv";
dotenv.config();

export default {
  ELASTICSEARCH_HOST: process.env.ELASTICSEARCH_HOST,
  INDEX_NAME: process.env.INDEX_NAME,
  ELASTIC_USERNAME: process.env.ELASTIC_USERNAME,
  ELASTIC_PASSWORD: process.env.ELASTIC_PASSWORD,
  REQUEST_HEADERS: {
    "User-Agent":
      process.env.USER_AGENT ||
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
  },
  COUNTRIES_URL:
    process.env.COUNTRIES_URL ||
    "https://www.worldometers.info/world-population/population-by-country/?t=" +
      Date.now(),
  WORLD_URL:
    process.env.WORLD_URL || "https://www.worldometers.info/world-population/",
};
