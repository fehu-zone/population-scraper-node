process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
import dotenv from "dotenv";
import { fetchCountryDataDynamic } from "./scraper/countryDataDynamic.js";
import { fetchWorldDataDynamic } from "./scraper/worldDataDynamic.js";
import { initIndex, markCurrentSnapshot, client } from "./elastic/client.js";

dotenv.config();

// Gelişmiş Loglama Sistemi
const logger = {
  info: (message) =>
    console.log(
      `\x1b[36mℹ️ [${new Date().toLocaleTimeString()}] ${message}\x1b[0m`
    ),
  success: (message) =>
    console.log(
      `\x1b[32m✅ [${new Date().toLocaleTimeString()}] ${message}\x1b[0m`
    ),
  error: (message) =>
    console.log(
      `\x1b[31m❌ [${new Date().toLocaleTimeString()}] ${message}\x1b[0m`
    ),
  warn: (message) =>
    console.log(
      `\x1b[33m⚠️ [${new Date().toLocaleTimeString()}] ${message}\x1b[0m`
    ),
};

// Veri Doğrulama
const validateData = (worldData, countryData) => {
  const errors = [];
  const EXPECTED_COUNTRIES = 235;

  // Dünya verisi kontrolleri
  const worldPopulationThreshold = 7_900_000_000; // Güncel dünya nüfus eşiği
  if (
    !worldData?.current_population ||
    worldData.current_population < worldPopulationThreshold
  ) {
    errors.push(
      `Geçersiz dünya nüfusu: ${
        worldData?.current_population?.toLocaleString() || "bilinmiyor"
      }`
    );
  }

  // Ülke verisi kontrolleri
  if (!countryData?.length) {
    errors.push("Hiç ülke verisi alınamadı");
  } else {
    const missingCount = EXPECTED_COUNTRIES - countryData.length;
    if (missingCount > 0) errors.push(`Eksik ülke: ${missingCount}`);

    // Kritik ülke kontrolleri
    const criticalCountries = ["China", "India", "United States"];
    const missingCritical = criticalCountries.filter(
      (c) => !countryData.some((d) => d.country === c)
    );
    if (missingCritical.length)
      errors.push(`Eksik kritik ülkeler: ${missingCritical.join(", ")}`);

    // Veri kalite kontrolü
    const invalidEntries = countryData.filter(
      (c) =>
        c.current_population <= 0 || isNaN(c.yearly_change) || isNaN(c.med_age)
    ).length;
    if (invalidEntries > 5)
      errors.push(`${invalidEntries} geçersiz veri içeren ülke`);
  }

  return { isValid: !errors.length, errors };
};

// Ana İşlem Akışı
const processData = async () => {
  try {
    logger.info("Scraping süreci başlatılıyor...");

    // Elasticsearch hazırlığı
    await initIndex();

    // Paralel veri çekme
    const [worldData, countryData] = await Promise.allSettled([
      fetchWorldDataDynamic(),
      fetchCountryDataDynamic(),
    ]);

    // Hata yönetimi
    const results = {
      world: worldData.status === "fulfilled" ? worldData.value : null,
      country: countryData.status === "fulfilled" ? countryData.value : [],
    };

    logger.info("════════════ DÜNYA VERİLERİ ════════════");
    if (results.world) {
      logger.info(
        `🌍 Toplam Nüfus: ${results.world.current_population?.toLocaleString()}`
      );
      logger.info(
        `👶 Bugünkü Doğum: ${results.world.births_today?.toLocaleString()}`
      );
      logger.info(
        `☠️ Bugünkü Ölüm: ${results.world.deaths_today?.toLocaleString()}`
      );
      logger.info(
        `📈 Net Büyüme: ${results.world.population_growth?.toLocaleString()}`
      );
      logger.info(
        `📅 Yıllık Doğum: ${(
          results.world.births_today * 365
        ).toLocaleString()}`
      );
      logger.info(`⏳ Zaman Damgası: ${results.world["@timestamp"]}`);
    } else {
      logger.error("Dünya verisi alınamadı!");
    }
    logger.info("════════════════════════════════════════");

    logger.info("════════════ ÜLKE VERİLERİ ════════════");
    if (results.country.length > 0) {
      logger.info(`✅ ${results.country.length} ülke verisi alındı`);
      logger.info(
        `🏆 İlk 3 Ülke: ${results.country
          .slice(0, 3)
          .map((c) => c.country)
          .join(", ")}`
      );
      logger.info(
        `📊 Ortalama Yaş: ${(
          results.country.reduce((sum, c) => sum + (c.med_age || 0), 0) /
          results.country.length
        ).toFixed(1)}`
      );
    } else {
      logger.error("⛔ Hiç ülke verisi alınamadı!");
    }

    // Validasyon
    const validation = validateData(results.world, results.country);
    if (!validation.isValid) {
      throw new Error(`Validasyon Hatası:\n${validation.errors.join("\n")}`);
    }

    // Elasticsearch'e yazma
    const bulkBody = results.country.flatMap((country) => [
      { index: { _index: process.env.INDEX_NAME } },
      {
        ...country,
        type: "country",
        is_current: true,
        "@timestamp": new Date().toISOString(),
      },
    ]);

    if (results.world) {
      bulkBody.unshift(
        { index: { _index: process.env.INDEX_NAME } },
        { ...results.world, type: "world", is_current: true }
      );
    }

    const { body: response } = await client.bulk({
      refresh: "wait_for",
      body: bulkBody,
    });

    // Hata analizi
    if (response.errors) {
      logger.warn(
        `Hatalı dokümanlar: ${
          response.items.filter((i) => i.index.error).length
        }`
      );
      response.items.slice(0, 3).forEach(({ index }) => {
        if (index.error) logger.error(`Hata: ${index.error.reason}`);
      });
    }

    logger.success(`Başarıyla kaydedildi: ${response.items.length} kayıt`);
  } catch (error) {
    logger.error(`Kritik Hata: ${error.message}`);
    logger.info("5 dakika sonra yeniden denenecek...");
    setTimeout(processData, 300_000);
  }
};

// Uygulama başlatma
console.clear();
processData();
setInterval(processData, 1_800_000);
