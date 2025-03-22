import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";

const WORLD_URL = "https://www.worldometers.info/world-population/";

export const fetchWorldData = async () => {
  try {
    const { data } = await axios.get(WORLD_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 15000,
    });

    const $ = cheerio.load(data);

    const extractNumber = (selector) => {
      return $(selector)
        .find(".rts-nr-int")
        .toArray()
        .map((el) => $(el).text().trim())
        .join("")
        .replace(/,/g, "");
    };

    return {
      current_population: parseNumber(
        extractNumber('span[rel="current_population"]')
      ),
      births_today: parseNumber(extractNumber('span[rel="births_today"]')),
      deaths_today: parseNumber(extractNumber('span[rel="dth1s_today"]')),
      population_growth: parseNumber(
        extractNumber('span[rel="absolute_growth"]')
      ),
      "@timestamp": new Date().toISOString(),
    };
  } catch (error) {
    console.error("Dünya verileri çekilirken hata:", error);
    return null;
  }
};
