import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { parseNumber, cleanCountryName } from "./utils.js";
import config from "../config/index.js";

puppeteer.use(StealthPlugin());

export const fetchCountryDataDynamic = async () => {
  let browser;
  let page;

  try {
    // Tarayıcı başlatma
    browser = await puppeteer.launch({
      headless: "new",
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-features=site-per-process",
        "--lang=en-US",
        "--window-size=1920,3000",
      ],
    });

    page = await browser.newPage();
    await page.setViewport({ width: 1920, height: 3000 });

    // Gelişmiş tarayıcı ayarları
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.159 Safari/537.36"
    );
    await page.setJavaScriptEnabled(true);
    await page.setDefaultNavigationTimeout(120000);

    // Sayfa yükleme
    console.log("Sayfa yükleniyor:", config.COUNTRIES_URL);
    await page.goto(config.COUNTRIES_URL, {
      waitUntil: "networkidle2",
      timeout: 120000,
    });

    // Yeni tablo yükleme mekanizması
    await page.waitForFunction(
      () => {
        const potentialTables = Array.from(document.querySelectorAll("table"));
        return potentialTables.some((table) => {
          const headers = Array.from(table.querySelectorAll("th"));
          return headers.some((th) => th.textContent.includes("Population"));
        });
      },
      { timeout: 45000 }
    );

    // Alternatif veri çekme yöntemi
    const tableData = await page.evaluate(() => {
      const tables = Array.from(document.querySelectorAll("table"));
      const targetTable = tables.find(
        (table) =>
          table.textContent.includes("Country") &&
          table.textContent.includes("Population")
      );

      return Array.from(targetTable.querySelectorAll("tbody tr")).map((row) => {
        const cells = Array.from(row.querySelectorAll("td"));
        return cells.map((cell) =>
          cell.textContent
            .replace(/\u00a0/g, " ") // Özel boşlukları temizle
            .trim()
        );
      });
    });

    // Veri işleme
    const processedData = tableData
      .map((row) => ({
        rank: parseNumber(row[0]),
        country: cleanCountryName(row[1]),
        current_population: parseNumber(row[2]),
        yearly_change: parseNumber(row[3], true),
        net_change: parseNumber(row[4]),
        migrants: parseNumber(row[7]),
        med_age: parseNumber(row[9]),
      }))
      .filter((item) => item.rank > 0);

    return processedData;
  } catch (error) {
    console.error("Son hata:", error);
    if (page) {
      await page.screenshot({
        path: `final-error-${Date.now()}.png`,
        fullPage: true,
      });
    }
    return null;
  } finally {
    if (browser) await browser.close();
  }
};
