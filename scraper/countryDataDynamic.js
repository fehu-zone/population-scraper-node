import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import * as cheerio from "cheerio";
puppeteer.use(StealthPlugin());
import config from "../config/index.js";
import { parseNumber } from "./utils.js";

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
    // Yüklenme göstergesinin kaybolmasını bekle
    await page.waitForFunction(
      () => {
        const loader = document.querySelector(".loading-overlay");
        return !loader || loader.style.display === "none";
      },
      { timeout: 90000 }
    );
    // Tablonun belirli bir satırının görünür olduğunu garanti altına al
    await page.waitForSelector("#example2 tbody tr:nth-child(100)", {
      visible: true,
      timeout: 180000,
    });
    // İlk hücrede beklenen içeriğin gelmesini bekle
    await page.waitForFunction(
      () => {
        const firstCell = document.querySelector(
          "#example2 tbody tr:first-child td:nth-child(2)"
        );
        return firstCell && firstCell.textContent.trim().length > 2;
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
  const maxRetries = 3;
  let attempt = 0;
  while (attempt < maxRetries) {
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

      // Sayfanın tamamen yüklenmesini sağlamak için networkidle0 kullan
      await page.goto(config.COUNTRY_URL, {
        waitUntil: "networkidle0",
        timeout: 120000,
      });

      // DOM'un stabil hale gelmesi için kısa bekleme
      await page.waitForTimeout(2000);

      // Sayfanın tamamını yüklemek için scroll yap
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

      // Tablonun HTML'sini al
      const tableHTML = await page.evaluate(() => {
        const tbody = document.querySelector("#example2 tbody");
        return tbody ? tbody.innerHTML : "";
      });
      const $ = cheerio.load(tableHTML);
      const rows = $("#example2 tbody tr");
      const countryData = [];
      rows.each((i, row) => {
        const cells = $(row).find("td");
        if (cells.length < 10) return;
        const rawCountry = $(cells[1]).text().trim();
        const population =
          Number(
            $(cells[2])
              .text()
              .replace(/[^\d.-]/g, "")
          ) || 0;
        if (population < 1000) return;
        countryData.push({
          country: COUNTRY_MAPPING[rawCountry] || rawCountry,
          current_population: population,
          yearly_change:
            Number(
              $(cells[3])
                .text()
                .replace(/[^\d.-]/g, "")
            ) || 0,
          net_change:
            Number(
              $(cells[4])
                .text()
                .replace(/[^\d.-]/g, "")
            ) || 0,
          migrants:
            Number(
              $(cells[7])
                .text()
                .replace(/[^\d.-]/g, "")
            ) || 0,
          med_age:
            Number(
              $(cells[9])
                .text()
                .replace(/[^\d.-]/g, "")
            ) || 0,
        });
      });

      // Ek validasyon: Kayıtların belirli kriterleri sağlaması
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
      attempt++;
      if (attempt < maxRetries) {
        console.warn(`Deneme ${attempt} başarısız oldu, yeniden deneniyor...`);
      } else {
        console.error("Max deneme sayısına ulaşıldı, boş veri döndürülüyor.");
        return [];
      }
    } finally {
      if (browser) await browser.close();
    }
  }
};
