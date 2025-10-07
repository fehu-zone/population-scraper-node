import puppeteer from "puppeteer-extra";
import StealthPlugin from "puppeteer-extra-plugin-stealth";
import { parseNumber, cleanCountryName, getCountryCode } from "./utils.js";
import config from "../config/index.js";
import { logger } from "../simple-logger.js";

puppeteer.use(StealthPlugin());

export const fetchCountryDataDynamic = async () => {
  let browser;
  let page;

  try {
    logger.info("Tarayıcı başlatılıyor");
    
    browser = await puppeteer.launch({
      headless: "new",
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

    // Tarayıcı ayarları
    await page.setUserAgent(
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.159 Safari/537.36"
    );
    await page.setDefaultNavigationTimeout(60000);
    
    // Performans için gereksiz kaynakları engelle
    await page.setRequestInterception(true);
    page.on('request', (req) => {
      const resourceType = req.resourceType();
      if (['image', 'stylesheet', 'font'].includes(resourceType)) {
        req.abort();
      } else {
        req.continue();
      }
    });

    // Sayfa yükleme
    logger.info(`Sayfa yükleniyor: ${config.COUNTRIES_URL}`);
    await page.goto(config.COUNTRIES_URL, {
      waitUntil: "domcontentloaded",
      timeout: 60000,
    });

    // Tabloyu bekle
    logger.info("Tablo yüklenmesi bekleniyor");
    await page.waitForSelector('table', { timeout: 30000 });
    
    await page.waitForFunction(
      () => {
        const tables = document.querySelectorAll('table');
        for (let table of tables) {
          const rows = table.querySelectorAll('tbody tr');
          if (rows.length > 50) return true;
        }
        return false;
      },
      { timeout: 30000 }
    );

    // ✅ DÜZELTİLMİŞ VERİ ÇEKME - TÜM SÜTUNLARI AL
    logger.info("Tablo verileri çekiliyor");
    
    const tableData = await page.evaluate(() => {
      const tables = Array.from(document.querySelectorAll('table'));
      let targetTable = null;
      
      // Doğru tabloyu bul
      for (let table of tables) {
        const headers = Array.from(table.querySelectorAll('th'));
        const hasPopulation = headers.some(th => 
          th.textContent.includes('Population')
        );
        const hasCountry = headers.some(th => 
          th.textContent.includes('Country')
        );
        
        if (hasPopulation && hasCountry) {
          const rows = table.querySelectorAll('tbody tr');
          if (rows.length > 50) {
            targetTable = table;
            break;
          }
        }
      }

      if (!targetTable) {
        targetTable = tables[0];
      }

      // ✅ TÜM SÜTUNLARI DOĞRU ŞEKİLDE ÇEK
      return Array.from(targetTable.querySelectorAll('tbody tr')).map((row) => {
        const cells = Array.from(row.querySelectorAll('td'));
        
        // ✅ CRITICAL FIX: data-country attribute'unu al
        const countryLink = cells[1]?.querySelector('a');
        const dataCountry = countryLink?.getAttribute('data-country');
        const countryText = cells[1]?.textContent.trim();
        
        return {
          // ✅ TEMEL ALANLAR
          rank: cells[0]?.textContent.trim(),
          country: dataCountry || countryText, // Önce data-country, sonra text
          country_slug: countryLink?.getAttribute('href')?.split('/').filter(Boolean).pop() || '',
          
          // ✅ NÜFUS VERİLERİ
          population: cells[2]?.textContent.trim(),
          yearly_change: cells[3]?.textContent.trim(),
          net_change: cells[4]?.textContent.trim(),
          density: cells[5]?.textContent.trim(),
          land_area: cells[6]?.textContent.trim(),
          migrants: cells[7]?.textContent.trim(),
          fert_rate: cells[8]?.textContent.trim(),
          med_age: cells[9]?.textContent.trim(),
          urban_pop: cells[10]?.textContent.trim(),
          world_share: cells[11]?.textContent.trim(),
          
          // ✅ HAM VERİLER (SADECE İŞLEME İÇİN, GÖNDERİLMEYECEK)
          _raw_cells: cells.map(cell => cell.textContent.trim())
        };
      });
    });

    // ✅ DÜZELTİLMİŞ VERİ İŞLEME - DEBUG OLMADAN
    logger.info("Veriler işleniyor");
    
    const processedData = tableData
      .map((row) => {
        try {
          // ✅ TEMEL VERİLER
          const rank = parseNumber(row.rank);
          const country = cleanCountryName(row.country);
          const country_slug = row.country_slug;
          const country_code = getCountryCode(country, country_slug);
          
          // ✅ NÜFUS VERİLERİ
          const current_population = parseNumber(row.population);
          const yearly_change = parseNumber(row.yearly_change, true);
          const net_change = parseNumber(row.net_change);
          const migrants = parseNumber(row.migrants);
          const med_age = parseNumber(row.med_age);
          const population_growth = parseNumber(row.fert_rate); // Fertility rate olarak kullan
          
          // ✅ EK VERİLER (density ve land_area hariç)
          const urban_population_percent = parseNumber(row.urban_pop, true);
          const world_share_percent = parseNumber(row.world_share, true);

          // ✅ GEÇERLİ VERİ KONTROLÜ
          if (!country || country === 'Unknown' || !current_population || current_population < 1000) {
            return null;
          }

          // ✅ TEMİZ VERİ OBJESİ - DEBUG OLMADAN
          return {
            rank,
            country,
            country_slug, 
            country_code,
            current_population,
            yearly_change,
            net_change,
            migrants,
            med_age,
            population_growth,
            urban_population_percent,
            world_share_percent
          };
        } catch (error) {
          logger.warn(`Satır işleme hatası: ${error.message}`);
          return null;
        }
      })
      .filter(item => item !== null);

    logger.data("Ülke verileri işlendi", processedData.length);

    // ✅ ÖRNEK VERİLERİ LOGLA (DEBUG OLMADAN)
    if (processedData.length > 0) {
      logger.info("İlk 3 örnek veri:");
      processedData.slice(0, 3).forEach((item, idx) => {
        logger.info(`${idx + 1}. ${item.country} (${item.country_code}) - ${item.current_population?.toLocaleString()}`);
      });
    }

    return processedData;

  } catch (error) {
    logger.error(`Kritik hata: ${error.message}`);
    return null;
  } finally {
    // Tarayıcıyı kapat
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