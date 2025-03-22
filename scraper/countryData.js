import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber, parsePercentage } from "./utils.js";

const COUNTRY_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  "Iran (Islamic Republic of)": "Iran",
  "Viet Nam": "Vietnam",
  Czechia: "Czech Republic",
};

export const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(config.COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      params: {
        order: "desc",
        orderby: "population",
      },
      timeout: 20000,
    });

    const $ = cheerio.load(data);
    const rows = $("#example2 tbody tr");

    const countryData = rows
      .map((i, row) => {
        const cells = $(row).find("td");
        if (cells.length < 10) return null;

        const rawCountry = $(cells[1]).text().trim();

        return {
          country: COUNTRY_MAPPING[rawCountry] || rawCountry,
          current_population: parseNumber($(cells[2]).text()),
          yearly_change: parsePercentage($(cells[3]).text()),
          net_change: parseNumber($(cells[4]).text()),
          migrants: parseNumber($(cells[7]).text()),
          med_age: parseNumber($(cells[9]).text()),
        };
      })
      .get()
      .filter(
        (item) =>
          item && item.current_population > 0 && !isNaN(item.yearly_change)
      );

    // Debug için ilk 3 kayıt
    console.log("Örnek Ülke Verileri:", countryData.slice(0, 3));

    return countryData;
  } catch (error) {
    console.error("Ülke verileri çekilirken hata:", error);
    return [];
  }
};
