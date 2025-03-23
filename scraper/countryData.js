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
  "South Korea": "Republic of Korea",
};

const validateCountryRow = (cells) => {
  if (cells.length < 10) return false;
  const requiredFields = [2, 3, 4, 7, 9];
  return requiredFields.every((idx) => {
    const text = cells.eq(idx).text().trim();
    return text && text !== "N/A" && text !== "";
  });
};

export const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(config.COUNTRY_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 20000,
    });

    const $ = cheerio.load(data);
    const rows = $("#example2 tbody tr");

    const countryData = rows
      .map((i, row) => {
        const cells = $(row).find("td");
        if (!validateCountryRow(cells)) return null;

        const rawCountry = cells.eq(1).text().trim();

        return {
          country: COUNTRY_MAPPING[rawCountry] || rawCountry,
          current_population: parseNumber(cells.eq(2).text()),
          yearly_change: parsePercentage(cells.eq(3).text()),
          net_change: parseNumber(cells.eq(4).text()),
          migrants: parseNumber(cells.eq(7).text()),
          med_age: parseNumber(cells.eq(9).text()),
        };
      })
      .get()
      .filter((item) => item && item.current_population > 0);

    console.log("Başarıyla çekilen ülke sayısı:", countryData.length);
    return countryData;
  } catch (error) {
    console.error("Ülke veri hatası:", error.response?.status || error.message);
    return [];
  }
};
