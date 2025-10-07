import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";
import { logger } from "../simple-logger.js";

puppeteer.use(StealthPlugin());

export const fetchWorldDataDynamic = async () => {
  let browser;
  let page;

  try {
    logger.info("Dünya verileri için tarayıcı başlatılıyor");
    
    browser = await puppeteer.launch({
      headless: "new",
      ignoreHTTPSErrors: true,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--disable-gpu",
        "--lang=en-US",
        "--window-size=1920,1080",
      ],
    });

    page = await browser.newPage();
    
    await page.setViewport({ width: 1920, height: 1080 });
    await page.setUserAgent(config.REQUEST_HEADERS["User-Agent"]);
    await page.setDefaultNavigationTimeout(45000);
    
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      const resourceType = req.resourceType();
      if (['image', 'stylesheet', 'font', 'media'].includes(resourceType)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    logger.info(`Sayfa yükleniyor: ${config.WORLD_URL}`);
    await page.goto(config.WORLD_URL, {
      waitUntil: "networkidle2",
      timeout: 45000,
    });

    // ✅ YENİ BEKLEME STRATEJİSİ - GRID YAPISINI BEKLE
    logger.info("Dünya veri elementleri bekleniyor");
    await page.waitForSelector('.grid.grid-cols-1.gap-5.md\\:grid-cols-2', { timeout: 20000 });

    // ✅ YENİ VERİ ÇEKME YÖNTEMİ - GRID YAPISINI PARSE ET
    logger.info("Dünya verileri çekiliyor");
    
    const result = await page.evaluate(() => {
      // ✅ TÜM COUNTER'LARI TOPLA
      const counters = Array.from(document.querySelectorAll('.rts-counter'));
      const data = {};
      
      counters.forEach(counter => {
        const rel = counter.getAttribute('rel');
        if (rel) {
          // Sayısal değeri birleştir
          const numberText = Array.from(counter.querySelectorAll('.rts-nr-int'))
            .map(el => el.textContent.trim())
            .join('')
            .replace(/,/g, ''); // Virgülleri kaldır
          data[rel] = numberText;
        }
      });

      // ✅ CURRENT POPULATION'ı AYRI BUL
      const worldPopulationElement = document.querySelector('.rts-counter[rel="current_population"]');
      if (worldPopulationElement) {
        const populationText = Array.from(worldPopulationElement.querySelectorAll('.rts-nr-int'))
          .map(el => el.textContent.trim())
          .join('')
          .replace(/,/g, '');
        data.current_population = populationText;
      }

      return {
        // ✅ BUGÜNÜN VERİLERİ
        current_population: data.current_population || "",
        births_today: data.births_today || "",
        dth1s_today: data.dth1s_today || "",
        absolute_growth: data.absolute_growth || "",
        
        // ✅ BU YILIN VERİLERİ
        births_this_year: data.births_this_year || "",
        dth1s_this_year: data.dth1s_this_year || "",
        absolute_growth_year: data.absolute_growth_year || "",

        "@timestamp": new Date().toISOString(),
      };
    });

    // ✅ DETAYLI DEBUG
    logger.info("HAM VERİLER:");
    logger.info(`- current_population: "${result.current_population}"`);
    logger.info(`- births_today: "${result.births_today}"`);
    logger.info(`- dth1s_today: "${result.dth1s_today}"`);
    logger.info(`- absolute_growth: "${result.absolute_growth}"`);
    logger.info(`- births_this_year: "${result.births_this_year}"`);
    logger.info(`- dth1s_this_year: "${result.dth1s_this_year}"`);
    logger.info(`- absolute_growth_year: "${result.absolute_growth_year}"`);

    // ✅ VERİ İŞLEME
    logger.info("Veriler işleniyor");
    
    const processedData = {
      // Temel nüfus
      current_population: parseNumber(result.current_population),

      // Bugün
      births_today: parseNumber(result.births_today),
      dth1s_today: parseNumber(result.dth1s_today),
      population_growth: parseNumber(result.absolute_growth),

      // Bu yıl
      births_this_year: parseNumber(result.births_this_year),
      dth1s_this_year: parseNumber(result.dth1s_this_year),
      absolute_growth: parseNumber(result.absolute_growth_year),
      
      // Sistem
      "@timestamp": result["@timestamp"],
      type: "world",
      is_current: true,
      data_source: "worldometers"
    };

    // ✅ İŞLENMİŞ VERİLERİ GÖSTER
    logger.info("İŞLENMİŞ VERİLER:");
    logger.info(`- Nüfus: ${processedData.current_population?.toLocaleString()}`);
    logger.info(`- Bugün Doğum: ${processedData.births_today?.toLocaleString()}`);
    logger.info(`- Bugün Ölüm: ${processedData.dth1s_today?.toLocaleString()}`);
    logger.info(`- Bugün Büyüme: ${processedData.population_growth?.toLocaleString()}`);

    // ✅ KRİTİK VERİ KONTROLÜ
    const hasCriticalData = processedData.current_population > 0 && 
                           processedData.births_today > 0 && 
                           processedData.dth1s_today > 0;

    if (!hasCriticalData) {
      throw new Error("Kritik dünya verileri alınamadı");
    }

    logger.success("Dünya verileri başarıyla alındı");
    return processedData;

  } catch (error) {
    logger.error(`Dünya veri çekme hatası: ${error.message}`);
    
    // Hata durumunda screenshot al
    if (page) {
      try {
        await page.screenshot({
          path: `debug-world-${Date.now()}.png`,
          fullPage: true,
        });
        logger.info("📸 Debug screenshot kaydedildi");
      } catch (e) {
        logger.warn("Screenshot alınamadı");
      }
    }
    
    return null;
  } finally {
    if (browser) {
      try {
        await browser.close();
        logger.info("Tarayıcı kapatıldı");
      } catch (closeError) {
        logger.warn(`Tarayıcı kapatma hatası: ${closeError.message}`);
      }
    }
  }
};