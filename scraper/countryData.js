import axios from "axios";
import * as cheerio from "cheerio"; // cheerio'yu bu şekilde içe aktar
import config from "../config/index.js";
import { parseNumber, parsePercentage } from "./utils.js";

const COUNTRY_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  // Diğer eşleşmeler...
};

const COUNTRY_URL =
  "https://www.worldometers.info/world-population/population-by-country";

export const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      params: {
        order: "desc",
        orderby: "population",
      },
    });

    const $ = cheerio.load(data);
    const rows = $("#example2 tbody tr");

    return rows
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
  } catch (error) {
    console.error("Ülke verileri çekilirken hata:", error.message);
    return [];
  }
};
