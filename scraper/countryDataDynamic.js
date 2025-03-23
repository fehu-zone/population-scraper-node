import puppeteer from "puppeteer";
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

export const fetchCountryDataDynamic = async () => {
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      ignoreHTTPSErrors: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--ignore-certificate-errors",
        "--disable-gpu",
        "--window-size=1920,1080",
      ],
    });

    const page = await browser.newPage();

    // Performans optimizasyonları
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      if (
        ["image", "stylesheet", "font", "media"].includes(req.resourceType())
      ) {
        req.abort();
      } else {
        req.continue();
      }
    });

    await page.setUserAgent(config.REQUEST_HEADERS["User-Agent"]);
    await page.setDefaultNavigationTimeout(120000);

    // Sayfa yükleme stratejisi
    await page.goto(config.COUNTRY_URL, {
      waitUntil: "domcontentloaded",
      timeout: 120000,
    });

    // Gelişmiş bekleme mekanizması
    try {
      await page.waitForFunction(
        () => {
          const table = document.querySelector("#example2");
          const rows = table?.querySelectorAll("tbody tr");
          return (
            rows?.length > 200 &&
            rows[0].querySelector("td:nth-child(2)")?.textContent?.trim() !== ""
          );
        },
        {
          timeout: 60000,
          polling: 1000,
        }
      );
    } catch (error) {
      await page.screenshot({ path: "timeout-error.png", fullPage: true });
      throw new Error(`Tablo yüklenemedi: ${error.message}`);
    }

    // Veri çekme
    const countryData = await page.evaluate(() => {
      const rows = Array.from(document.querySelectorAll("#example2 tbody tr"));
      return rows.map((row) => {
        const cells = Array.from(row.querySelectorAll("td"));
        return {
          country: cells[1]?.textContent?.trim(),
          population: cells[2]?.textContent?.trim(),
          yearlyChange: cells[3]?.textContent?.trim(),
          netChange: cells[4]?.textContent?.trim(),
          migrants: cells[7]?.textContent?.trim(),
          medAge: cells[9]?.textContent?.trim(),
        };
      });
    });

    // Veri temizleme
    return countryData
      .map((item) => ({
        country: COUNTRY_MAPPING[item.country] || item.country,
        current_population: parseNumber(item.population),
        yearly_change: parsePercentage(item.yearlyChange),
        net_change: parseNumber(item.netChange),
        migrants: parseNumber(item.migrants),
        med_age: parseNumber(item.medAge),
      }))
      .filter((item) => item.country && item.current_population > 0);
  } catch (error) {
    console.error("Ülke veri hatası:", error);
    return [];
  } finally {
    if (browser) await browser.close();
  }
};
