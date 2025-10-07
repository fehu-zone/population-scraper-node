import { client } from "../elastic/client.js";
import config from "../config/index.js";
import { logger } from "../simple-logger.js";

// ✅ BASİT SAYI PARSING
export const parseNumber = (str, isPercentage = false) => {
  if ([null, undefined, "", "-", "−", "N/A"].includes(str)) return 0;

  try {
    let cleaned = String(str)
      .replace(/[^\d.-]/g, "")
      .replace(/^\-/g, "-")
      .replace(/\-\-/g, "-")
      .trim();

    if (!cleaned) return 0;

    const number = parseFloat(cleaned);
    
    if (Number.isNaN(number)) {
      logger.warn(`Sayı parsing hatası: "${str}" -> NaN`);
      return 0;
    }

    // Yüzde değerleri için 100'e böl
    if (isPercentage && String(str).includes('%')) {
      return number / 100;
    }

    return number;
  } catch (error) {
    logger.warn(`Sayı parsing hatası: "${str}" -> ${error.message}`);
    return 0;
  }
};

// ✅ BASİT ÜLKE İSİM TEMİZLEME
export const cleanCountryName = (name) => {
  if (!name) return 'Unknown';
  
  const COUNTRY_NAME_MAPPING = {
    "United States": "United States",
    "USA": "United States",
    "UK": "United Kingdom", 
    "U.K.": "United Kingdom",
    "UAE": "United Arab Emirates",
    "DR Congo": "Democratic Republic of the Congo",
    "Congo (Kinshasa)": "Democratic Republic of the Congo",
    "Congo (Brazzaville)": "Republic of the Congo",
    "Ivory Coast": "Côte d'Ivoire",
    "Swaziland": "Eswatini",
    "Burma": "Myanmar",
    "Czech Republic": "Czechia",
    "Macedonia": "North Macedonia",
  };

  const mappedName = COUNTRY_NAME_MAPPING[name];
  if (mappedName) return mappedName;

  return name
    .replace(/\[.*?\]/g, "")
    .replace(/\(.*?\)/g, "")  
    .replace(/\s+/g, " ")
    .replace(/^\s+|\s+$/g, "")
    .trim();
};

// ✅ COUNTRY_CODE GENERATOR
export const getCountryCode = (countryName, countrySlug) => {
  const COUNTRY_CODE_MAP = {
    "india": "IN",
    "china": "CN", 
    "united states": "US",
    "united-states": "US",
    "indonesia": "ID",
    "pakistan": "PK",
    "nigeria": "NG",
    "brazil": "BR",
    "bangladesh": "BD",
    "russia": "RU",
    "mexico": "MX",
    "ethiopia": "ET",
    "japan": "JP",
    "philippines": "PH",
    "egypt": "EG",
    "vietnam": "VN",
    "turkey": "TR",
    "iran": "IR",
    "germany": "DE",
    "thailand": "TH",
    "united kingdom": "GB",
    "france": "FR",
    "italy": "IT",
    "south africa": "ZA",
    "south korea": "KR",
    "spain": "ES",
    "argentina": "AR",
    "algeria": "DZ",
    "sudan": "SD",
    "ukraine": "UA",
    "iraq": "IQ",
    "afghanistan": "AF",
    "poland": "PL",
    "canada": "CA",
    "morocco": "MA",
    "saudi arabia": "SA",
    "uzbekistan": "UZ",
    "peru": "PE",
    "angola": "AO",
    "malaysia": "MY",
    "mozambique": "MZ",
    "ghana": "GH",
    "yemen": "YE",
    "nepal": "NP",
    "venezuela": "VE",
    "madagascar": "MG",
    "cameroon": "CM",
    "ivory coast": "CI",
    "north korea": "KP",
    "australia": "AU",
    "taiwan": "TW"
  };

  // Önce country_slug'dan dene
  if (countrySlug) {
    const slugKey = countrySlug.replace('-population', '');
    if (COUNTRY_CODE_MAP[slugKey]) {
      return COUNTRY_CODE_MAP[slugKey];
    }
  }

  // Sonra country name'den dene
  if (countryName) {
    const nameKey = countryName.toLowerCase();
    if (COUNTRY_CODE_MAP[nameKey]) {
      return COUNTRY_CODE_MAP[nameKey];
    }
  }

  return '';
};

// ✅ BASİT BULK INDEXING - HATA DÜZELTİLMİŞ
export const bulkIndexCountries = async (countries, indexName = null) => {
  try {
    if (!Array.isArray(countries) || countries.length === 0) {
      throw new Error("Geçersiz veya boş ülke verisi");
    }

    const targetIndex = indexName || process.env.INDEX_NAME || config.INDEX_NAME;
    
    if (!targetIndex) {
      throw new Error("Index ismi belirtilmemiş");
    }

    // Önce mevcut current flag'leri kaldır
    await client.updateByQuery({
      index: targetIndex,
      conflicts: "proceed",
      refresh: true,
      body: {
        script: {
          source: "ctx._source.is_current = false",
          lang: "painless"
        },
        query: {
          term: { is_current: true }
        }
      }
    });

    // Bulk işlem için body hazırla (DEBUG OLMADAN)
    const body = countries.flatMap((country) => {
      if (!country || !country.country) {
        logger.warn("Geçersiz ülke verisi atlandı");
        return [];
      }

      const timestamp = new Date().toISOString();
      
      // ✅ DEBUG VERİLERİNİ KALDIR
      const { _debug, _raw_cells, ...cleanCountry } = country;
      
      return [
        { 
          index: { 
            _index: targetIndex,
            _id: `${cleanCountry.country_slug || cleanCountry.country}_${timestamp}`
          } 
        },
        {
          ...cleanCountry,
          type: "country",
          is_current: true,
          "@timestamp": timestamp
        }
      ];
    });

    if (body.length === 0) {
      throw new Error("İşlenecek veri bulunamadı");
    }

    // ✅ DÜZELTME: bulkResponse kullan
    const { body: bulkResponse } = await client.bulk({
      refresh: true,
      body
    });

    // Hataları kontrol et
    let successCount = 0;
    let errorCount = 0;

    // ✅ DÜZELTME: bulkResponse kullan
    if (bulkResponse.items) {
      bulkResponse.items.forEach(item => {
        if (item.index && item.index.error) {
          errorCount++;
        } else {
          successCount++;
        }
      });
    }

    if (errorCount > 0) {
      logger.warn(`Bulk işlem hataları: ${errorCount} kayıt`);
    }

    logger.data("Ülke verileri indexlendi", successCount);

    return {
      success: true,
      indexedCount: successCount,
      totalCount: countries.length,
      errors: errorCount
    };

  } catch (error) {
    logger.error(`Bulk index hatası: ${error.message}`);
    return {
      success: false,
      error: error.message,
      indexedCount: 0,
      totalCount: countries.length || 0
    };
  }
};

// ✅ BASİT VALIDATION
export const validateCountryData = (country) => {
  const errors = [];
  
  if (!country.country) errors.push("Ülke ismi eksik");
  if (!country.current_population) errors.push("Nüfus verisi eksik");
  if (country.current_population < 1000) errors.push("Geçersiz nüfus değeri");
  
  return {
    isValid: errors.length === 0,
    errors
  };
};

// ✅ BASİT SAMPLE LOGGER
export const logSampleData = (data, sampleSize = 3) => {
  if (!Array.isArray(data) || data.length === 0) {
    logger.info("Örnek veri: Yok");
    return;
  }
  
  logger.info(`İlk ${sampleSize} örnek veri:`);
  data.slice(0, sampleSize).forEach((item, index) => {
    logger.info(`${index + 1}. ${item.country} - ${item.current_population?.toLocaleString()}`);
  });
};