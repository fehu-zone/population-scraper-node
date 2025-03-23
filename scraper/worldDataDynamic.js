import puppeteer from "puppeteer";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";

export const fetchWorldDataDynamic = async () => {
  let browser;
  try {
    browser = await puppeteer.launch({
      headless: "new",
      ignoreHTTPSErrors: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
      ],
    });

    const page = await browser.newPage();
    await page.setUserAgent(config.REQUEST_HEADERS["User-Agent"]);
    await page.setDefaultNavigationTimeout(30000);

    await page.goto(config.WORLD_URL, {
      waitUntil: "networkidle2",
      timeout: 30000,
    });

    // Daha güvenilir veri çekme
    const result = await page.evaluate(() => {
      const getValue = (rel) => {
        const el = document.querySelector(`[rel='${rel}']`);
        return el
          ? Array.from(el.querySelectorAll(".rts-nr-int"))
              .map((e) => e.textContent.trim())
              .join("")
          : "";
      };

      return {
        current_population: getValue("current_population"),
        births_today: getValue("births_today"),
        deaths_today: getValue("dth1s_today"),
        population_growth: getValue("absolute_growth"),
        "@timestamp": new Date().toISOString(),
      };
    });

    return {
      current_population: parseNumber(result.current_population),
      births_today: parseNumber(result.births_today),
      deaths_today: parseNumber(result.deaths_today),
      population_growth: parseNumber(result.population_growth),
      "@timestamp": result["@timestamp"],
    };
  } catch (error) {
    console.error("Dünya veri hatası:", error);
    return null;
  } finally {
    if (browser) await browser.close();
  }
};
