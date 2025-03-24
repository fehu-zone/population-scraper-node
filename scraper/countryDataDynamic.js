import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
puppeteer.use(StealthPlugin());
import config from "../config/index.js";
import { parseNumber, parsePercentage } from "./utils.js";

const browserConfig = {
  headless: "new",
  ignoreHTTPSErrors: true,
  args: [
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--disable-infobars",
    "--single-process",
    "--no-zygote",
    "--disable-notifications",
    "--disable-web-security",
    "--disable-features=IsolateOrigins,site-per-process",
    "--window-size=1920,1080",
    "--lang=en-US,en",
  ],
  defaultViewport: null,
  ignoreDefaultArgs: ["--disable-extensions"],
};

const COUNTRY_MAPPING = {
  "United States": "USA",
  "Congo (Dem. Rep.)": "DR Congo",
  Iran: "Iran",
  Vietnam: "Vietnam",
  Czechia: "Czech Republic",
  "Korea, South": "South Korea",
};

const waitForTable = async (page) => {
  try {
    // Önce yüklenme göstergesini bekle
    await page.waitForFunction(
      () => {
        const loader = document.querySelector(".loading-overlay");
        return !loader || loader.style.display === "none";
      },
      { timeout: 90000 }
    );

    // Tablo için optimize edilmiş bekleme
    await page.waitForSelector("#example2 tbody tr:nth-child(100)", {
      visible: true,
      timeout: 180000,
    });

    await page.waitForFunction(
      () => {
        const firstCell = document.querySelector(
          "#example2 tbody tr:first-child td:nth-child(2)"
        );
        return firstCell?.textContent?.trim().length > 2;
      },
      { timeout: 60000 }
    );
  } catch (error) {
    await page.screenshot({
      path: `table-error-${Date.now()}.png`,
      fullPage: true,
    });
    throw new Error(`Tablo yüklenemedi: ${error.message}`);
  }
};

export const fetchCountryDataDynamic = async () => {
  let browser;
  try {
    browser = await puppeteer.launch(browserConfig);
    const page = await browser.newPage();

    await page.setExtraHTTPHeaders({
      "Accept-Language": "en-US,en;q=0.9",
      Referer: config.COUNTRY_URL,
    });

    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    );

    // Navigasyon stratejisi
    await page.goto(config.COUNTRY_URL, {
      waitUntil: "domcontentloaded",
      timeout: 120000,
    });

    // Scroll optimizasyonu
    let lastHeight = 0;
    for (let i = 0; i < 5; i++) {
      const newHeight = await page.evaluate(async () => {
        window.scrollTo(0, document.body.scrollHeight);
        await new Promise((resolve) => setTimeout(resolve, 2000));
        return document.body.scrollHeight;
      });

      if (newHeight === lastHeight) break;
      lastHeight = newHeight;
    }

    await waitForTable(page);

    const countryData = await page.evaluate((COUNTRY_MAPPING) => {
      const parseCell = (cell, isNumber) => {
        const text = cell?.textContent?.trim().replace(/,/g, "");
        if (!text) return null;
        return isNumber
          ? Number(text.replace(/[^\d.-]/g, ""))
          : text.replace(/\\n/g, "");
      };

      return Array.from(document.querySelectorAll("#example2 tbody tr"))
        .map((row) => {
          const cells = row.querySelectorAll("td");
          if (cells.length < 10) return null;

          const rawCountry = parseCell(cells[1], false);
          const population = parseCell(cells[2], true);

          if (!population || population < 1000) return null;

          return {
            country: COUNTRY_MAPPING[rawCountry] || rawCountry,
            current_population: population,
            yearly_change: parseCell(cells[3], true),
            net_change: parseCell(cells[4], true),
            migrants: parseCell(cells[7], true),
            med_age: parseCell(cells[9], true),
          };
        })
        .filter((item) => item?.country);
    }, COUNTRY_MAPPING);

    // Ek veri validasyonu
    const validData = countryData.filter(
      (item) =>
        item.current_population > 1000 &&
        item.med_age > 10 &&
        Math.abs(item.yearly_change) < 100
    );

    if (validData.length < 50) {
      throw new Error(`Yetersiz veri: ${validData.length} geçerli kayıt`);
    }

    return validData;
  } catch (error) {
    console.error("Gelişmiş hata logu:", {
      message: error.message,
      url: config.COUNTRY_URL,
      stack: error.stack,
    });
    return [];
  } finally {
    if (browser) await browser.close();
  }
};
