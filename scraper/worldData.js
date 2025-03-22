import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";

const WORLD_URL = "https://www.worldometers.info/world-population/";

export const fetchWorldData = async () => {
  try {
    const { data } = await axios.get(WORLD_URL, {
      headers: config.REQUEST_HEADERS,
    });

    const $ = cheerio.load(data);

    return {
      current_population: parseNumber($(".rts-counter span").first().text()),
      births_today: parseNumber(
        $('div:contains("Births today")')
          .closest(".col-md-3")
          .find(".rts-counter")
          .text()
      ),
      deaths_today: parseNumber(
        $('div:contains("Deaths today")')
          .closest(".col-md-3")
          .find(".rts-counter")
          .text()
      ),
    };
  } catch (error) {
    console.error("Dünya verileri çekilirken hata:", error.message);
    return null;
  }
};
