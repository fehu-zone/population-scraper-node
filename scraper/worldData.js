import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";

export const fetchWorldData = async () => {
  try {
    const { data } = await axios.get(config.WORLD_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 15000,
    });

    const $ = cheerio.load(data);

    const extractValue = (relAttr) => {
      const element = $(`span[rel="${relAttr}"]`);
      if (!element.length) return null;

      return parseNumber(
        element
          .find(".rts-nr-int")
          .toArray()
          .map((el) => $(el).text().trim())
          .join("")
      );
    };

    const result = {
      current_population: extractValue("current_population"),
      births_today: extractValue("births_today"),
      deaths_today: extractValue("dth1s_today"),
      population_growth: extractValue("absolute_growth"),
      "@timestamp": new Date().toISOString(),
    };

    // Veri kalite kontrolü
    if (Object.values(result).some((v) => v === null || Number.isNaN(v))) {
      throw new Error("Eksik veya geçersiz dünya verileri");
    }

    return result;
  } catch (error) {
    console.error("Dünya veri hatası:", error.message);
    return null;
  }
};
