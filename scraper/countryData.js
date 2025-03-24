import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber, parsePercentage } from "./utils.js";
import axiosRetry from "axios-retry";

// Retry mekanizmasını kur
axiosRetry(axios, {
  retries: 3,
  retryDelay: (retryCount) => retryCount * 2000,
  retryCondition: (error) =>
    error.code === "ECONNABORTED" || error.response?.status >= 500,
});

const COUNTRY_MAPPING = {
  "United States": "USA",
  Congo: "DR Congo",
  "Iran (Islamic Republic of)": "Iran",
  "Viet Nam": "Vietnam",
  Czechia: "Czech Republic",
  "South Korea": "Republic of Korea",
  "Russian Federation": "Russia",
  "Syrian Arab Republic": "Syria",
};

const validateCountryRow = (cells) => {
  if (cells.length < 10) return false;
  const requiredFields = [1, 2, 3, 4, 7, 9]; // Ülke adı dahil
  return requiredFields.every((idx) => {
    const text = cells.eq(idx).text().trim();
    return text && !text.includes("NaN") && text !== "N/A";
  });
};

export const fetchCountryData = async () => {
  try {
    const { data } = await axios.get(config.COUNTRY_URL, {
      headers: {
        ...config.REQUEST_HEADERS,
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        Referer: config.COUNTRY_URL,
      },
      timeout: 40000,
    });

    const $ = cheerio.load(data);

    // Tablo varlık kontrolü
    const table = $("#example2");
    if (!table.length) {
      throw new Error("Hedef tablo bulunamadı!");
    }

    const rows = table.find("tbody tr");
    if (rows.length < 150) {
      throw new Error(`Yetersiz veri: Sadece ${rows.length} satır bulundu`);
    }

    const countryData = rows
      .map((i, row) => {
        const cells = $(row).find("td");
        if (!validateCountryRow(cells)) return null;

        const rawCountry = cells.eq(1).text().trim().replace(/\\n/g, "");

        const data = {
          country: COUNTRY_MAPPING[rawCountry] || rawCountry,
          current_population: parseNumber(cells.eq(2).text()),
          yearly_change: parsePercentage(cells.eq(3).text()),
          net_change: parseNumber(cells.eq(4).text()),
          migrants: parseNumber(cells.eq(7).text()),
          med_age: parseNumber(cells.eq(9).text()),
        };

        // Veri kalite kontrolü
        if (
          data.current_population < 1000 ||
          Math.abs(data.yearly_change) > 100 ||
          data.med_age < 10
        ) {
          return null;
        }

        return data;
      })
      .get()
      .filter(Boolean);

    if (countryData.length < 50) {
      throw new Error(
        `Yetersiz veri: Sadece ${countryData.length} ülke alınabildi`
      );
    }

    console.log("Başarıyla çekilen ülke sayısı:", countryData.length);
    return countryData;
  } catch (error) {
    console.error("Ülke veri hatası:", {
      message: error.message,
      url: config.COUNTRY_URL,
      status: error.response?.status,
      stack: error.stack,
    });
    return [];
  }
};
