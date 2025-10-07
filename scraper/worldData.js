import axios from "axios";
import * as cheerio from "cheerio";
import config from "../config/index.js";
import { parseNumber } from "./utils.js";
import { logger } from "../simple-logger.js";

export const fetchWorldData = async () => {
  try {
    logger.info("Dünya verileri çekiliyor (Axios)");
    
    const { data } = await axios.get(config.WORLD_URL, {
      headers: config.REQUEST_HEADERS,
      timeout: 30000,
      maxRedirects: 3
    });

    const $ = cheerio.load(data);

    // ✅ YENİ DEĞER ÇEKME FONKSİYONU - .rts-counter kullan
    const extractValue = (relAttr) => {
      try {
        // ÖNCE: .rts-counter[rel="${relAttr}"] ara (YENİ YAPI)
        let element = $(`.rts-counter[rel="${relAttr}"]`);
        
        // SONRA: span[rel="${relAttr}"] ara (ESKİ YAPI)
        if (!element.length) {
          element = $(`span[rel="${relAttr}"]`);
        }
        
        if (!element.length) {
          logger.warn(`Element bulunamadı: ${relAttr}`);
          return null;
        }

        const numberText = element
          .find(".rts-nr-int")
          .toArray()
          .map((el) => $(el).text().trim())
          .join("")
          .replace(/,/g, ""); // Virgülleri kaldır

        const value = parseNumber(numberText);
        
        logger.info(`${relAttr}: "${numberText}" -> ${value}`);
        return value;
      } catch (error) {
        logger.warn(`${relAttr} çekme hatası: ${error.message}`);
        return null;
      }
    };

    // ✅ ALTERNATİF YÖNTEM - TÜM COUNTER'LARI TOPLA
    const extractAllCounters = () => {
      const counters = {};
      
      $('.rts-counter[rel]').each((index, element) => {
        const rel = $(element).attr('rel');
        const numberText = $(element)
          .find('.rts-nr-int')
          .toArray()
          .map(el => $(el).text().trim())
          .join('')
          .replace(/,/g, '');
        
        if (rel && numberText) {
          counters[rel] = parseNumber(numberText);
        }
      });
      
      return counters;
    };

    logger.info("Dünya verileri işleniyor");

    // ✅ ÖNCE YENİ YÖNTEMLE DENE
    let result = {
      current_population: extractValue("current_population"),
      births_today: extractValue("births_today"),
      dth1s_today: extractValue("dth1s_today"),
      absolute_growth: extractValue("absolute_growth"),
      births_this_year: extractValue("births_this_year"),
      dth1s_this_year: extractValue("dth1s_this_year"),
      absolute_growth_year: extractValue("absolute_growth_year"),
    };

    // ✅ EĞER VERİ GELMEDİYSE, ALTERNATİF YÖNTEMİ DENE
    const hasData = Object.values(result).some(val => val && val > 0);
    
    if (!hasData) {
      logger.warn("Birinci yöntem başarısız, alternatif yöntem deneniyor");
      const counterData = extractAllCounters();
      
      result = {
        current_population: counterData.current_population,
        births_today: counterData.births_today,
        dth1s_today: counterData.dth1s_today,
        absolute_growth: counterData.absolute_growth,
        births_this_year: counterData.births_this_year,
        dth1s_this_year: counterData.dth1s_this_year,
        absolute_growth_year: counterData.absolute_growth_year,
      };
    }

    // ✅ DEBUG: HANGİ VERİLERİN GELDİĞİNİ GÖSTER
    logger.info("ALINAN VERİLER:");
    Object.entries(result).forEach(([key, value]) => {
      logger.info(`- ${key}: ${value}`);
    });

    // ✅ TEMİZ VERİ OBJESİ (DEBUG OLMADAN)
    const finalResult = {
      // Temel nüfus
      current_population: result.current_population,

      // Bugün
      births_today: result.births_today,
      dth1s_today: result.dth1s_today,
      population_growth: result.absolute_growth, // absolute_growth -> population_growth

      // Bu yıl
      births_this_year: result.births_this_year,
      dth1s_this_year: result.dth1s_this_year,
      absolute_growth: result.absolute_growth_year,
      
      // Sistem
      "@timestamp": new Date().toISOString(),
      type: "world",
      is_current: true,
      data_source: "worldometers"
    };

    // ✅ VERİ KALİTE KONTROLÜ
    const requiredFields = ['current_population', 'births_today', 'dth1s_today'];
    const missingFields = requiredFields.filter(field => 
      !finalResult[field] || finalResult[field] === 0
    );

    if (missingFields.length > 0) {
      logger.warn(`Eksik alanlar: ${missingFields.join(', ')}`);
      
      // Mevcut tüm rel'leri listele (debug için)
      const allRels = $('.rts-counter[rel]')
        .map((i, el) => $(el).attr('rel'))
        .get();
      logger.warn(`Mevcut rel'ler: ${allRels.join(', ')}`);
      
      if (missingFields.includes('current_population')) {
        throw new Error("Kritik nüfus verisi alınamadı");
      }
    }

    // ✅ BAŞARILI SONUÇ
    logger.success("Dünya verileri başarıyla alındı");
    logger.data("Dünya nüfusu", 1, `${finalResult.current_population?.toLocaleString()}`);
    
    return finalResult;

  } catch (error) {
    logger.error(`Dünya veri çekme hatası: ${error.message}`);
    return null;
  }
};

// ✅ YEDEK FONKSİYON (BASİT)
export const fetchWorldDataBackup = async () => {
  try {
    logger.info("Yedek dünya veri kaynağı deneniyor");
    return null;
  } catch (error) {
    logger.error(`Yedek dünya veri hatası: ${error.message}`);
    return null;
  }
};