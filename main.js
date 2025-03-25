process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
import dotenv from "dotenv";
import { fetchCountryDataDynamic } from "./scraper/countryDataDynamic.js";
import { fetchWorldDataDynamic } from "./scraper/worldDataDynamic.js";
import { initIndex, client } from "./elastic/client.js";
import ProgressBar from "progress";
import { updateCurrentSnapshot } from "./elastic/client.js";

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

// Geliştirilmiş Veri Doğrulama
const validateData = (worldData, countryData) => {
  const warnings = [];
  const errors = [];
  const EXPECTED_COUNTRIES = 235;

  // Dünya verisi kontrolleri
  if (!worldData?.current_population) {
    errors.push("Dünya nüfus verisi eksik");
  }

  // Ülke verisi kontrolleri
  if (!countryData || countryData.length === 0) {
    errors.push("Hiç ülke verisi alınamadı");
    return { isValid: false, errors, warnings };
  }

  const totalCountries = countryData.length;
  const validCountries = countryData.filter(
    (c) =>
      c.current_population > 0 && !isNaN(c.yearly_change) && !isNaN(c.med_age)
  ).length;

  // Uyarılar
  if (totalCountries < EXPECTED_COUNTRIES) {
    warnings.push(`Eksik ülke: ${EXPECTED_COUNTRIES - totalCountries}`);
  }

  const criticalMissing = ["China", "India", "United States"].filter(
    (c) => !countryData.some((d) => d.country === c)
  );

  if (criticalMissing.length > 0) {
    warnings.push(`Eksik kritik ülkeler: ${criticalMissing.join(", ")}`);
  }

  if (totalCountries - validCountries > 0) {
    warnings.push(
      `Geçersiz veri içeren ülkeler: ${totalCountries - validCountries}`
    );
  }

  // Hatalar
  if (validCountries === 0) {
    errors.push("Hiç geçerli ülke verisi yok");
  }

  return {
    isValid: errors.length === 0,
    errors,
    warnings,
  };
};

// Ana İşlem Akışı
const processData = async () => {
  try {
    logger.info("Scraping süreci başlatılıyor...");

    // Elasticsearch hazırlığı
    await initIndex();

    // 1. Dünya verilerini çek
    logger.info("════════════ DÜNYA VERİLERİ ÇEKİLİYOR ════════════");
    const worldData = await fetchWithProgress(
      fetchWorldDataDynamic,
      "🌍 Dünya verisi",
      15,
      120000
    );

    // 2. Bekleme süresi
    logger.info("Dünya verisi alındıktan sonra 20 saniye bekleniyor...");
    await delay(20000);

    // 3. Ülke verilerini çek
    logger.info("════════════ ÜLKE VERİLERİ ÇEKİLİYOR ════════════");
    const countryData = await fetchWithProgress(
      fetchCountryDataDynamic,
      "🌐 Ülke verisi",
      30,
      240000
    );

    // Sonuçları işle
    const results = { world: worldData, country: countryData };
    logResults(results);

    // Validasyon
    const validation = validateData(results.world, results.country);
    handleValidation(validation);

    // Elasticsearch'e gönder
    const { successCount, errorCount } = await sendToElastic(results);

    logger.success(`Başarıyla kaydedildi: ${successCount} kayıt`);
    if (errorCount > 0) {
      logger.warn(`Başarısız kayıtlar: ${errorCount}`);
    }

    // Snapshot güncelleme
    await updateCurrentSnapshot(new Date().toISOString());
  } catch (error) {
    logger.error(`Kritik Hata: ${error.message}`);
    logger.info("5 dakika sonra yeniden denenecek...");
    setTimeout(processData, 300000);
  }
};

// Yardımcı Fonksiyonlar
const fetchWithProgress = async (fetchFn, label, total, timeout) => {
  const bar = new ProgressBar(`${label} [:bar] :percent :etas`, {
    complete: "=",
    incomplete: " ",
    width: 30,
    total,
  });

  const timer = setInterval(() => bar.tick(), 1000);

  try {
    const result = await Promise.race([
      fetchFn(),
      new Promise((_, reject) =>
        setTimeout(() => reject(new Error(`${label} zaman aşımı`)), timeout)
      ),
    ]);

    clearInterval(timer);
    bar.update(1);
    return result;
  } catch (error) {
    clearInterval(timer);
    throw error;
  }
};

const logResults = (results) => {
  logger.info("════════════ DÜNYA VERİLERİ ════════════");
  if (results.world) {
    logger.info(
      `🌍 Nüfus: ${results.world.current_population?.toLocaleString()}`
    );
    logger.info(
      `📈 Günlük Büyüme: ${results.world.population_growth?.toLocaleString()}`
    );
    logger.info(`⏳ Zaman Damgası: ${results.world["@timestamp"]}`);
  } else {
    logger.error("Dünya verisi yok");
  }

  logger.info("════════════ ÜLKE VERİLERİ ════════════");
  if (results.country?.length > 0) {
    logger.info(`✅ Toplam Ülke: ${results.country.length}`);
    logger.info(
      `🏆 İlk 3 Ülke: ${results.country
        .slice(0, 3)
        .map((c) => c.country)
        .join(", ")}`
    );
    logger.info(`📊 Ortalama Yaş: ${calculateAverageAge(results.country)}`);
  } else {
    logger.error("Ülke verisi yok");
  }
};

const handleValidation = ({ isValid, errors, warnings }) => {
  if (!isValid) {
    logger.error("Validasyon Hataları:");
    errors.forEach((e) => logger.error(`❌ ${e}`));
    throw new Error("Kritik validasyon hataları");
  }

  if (warnings.length > 0) {
    logger.warn("Validasyon Uyarıları:");
    warnings.forEach((w) => logger.warn(`⚠️  ${w}`));
  }
};

// main.js içinde sendToElastic fonksiyonu güncellemesi
const sendToElastic = async ({ world, country }) => {
  const body = [];

  try {
    // Dünya verisini ekle
    if (world) {
      body.push(
        { index: { _index: process.env.INDEX_NAME } },
        {
          ...world,
          type: "world",
          is_current: true,
          "@timestamp": new Date().toISOString(),
        }
      );
    }

    // Ülke verilerini ekle
    if (country?.length > 0) {
      country.forEach((c) => {
        body.push(
          { index: { _index: process.env.INDEX_NAME } },
          {
            ...c,
            type: "country",
            is_current: true,
            "@timestamp": new Date().toISOString(),
            current_population: c.current_population || 0,
            yearly_change: c.yearly_change || 0,
            net_change: c.net_change || 0,
            migrants: c.migrants || 0,
            med_age: c.med_age || 0,
          }
        );
      });
    }

    // Body kontrolü
    if (body.length === 0) {
      logger.warn("Gönderilecek veri yok");
      return { successCount: 0, errorCount: 0 };
    }

    const { body: response } = await client.bulk({
      refresh: "wait_for",
      body,
    });

    // Hata analizi
    let successCount = 0;
    let errorCount = 0;
    const errors = [];

    if (response?.items) {
      response.items.forEach((item, index) => {
        if (item.index.error) {
          errorCount++;
          errors.push({
            document: body[index * 2 + 1],
            reason: item.index.error.reason,
          });
        } else {
          successCount++;
        }
      });
    }

    // Hata loglama
    if (errorCount > 0) {
      logger.error(`İlk 3 hata detayı:`);
      errors.slice(0, 3).forEach((err, i) => {
        logger.error(`${i + 1}. Hata: ${err.reason}`);
        logger.error(`Belge: ${JSON.stringify(err.document)}`);
      });
    }

    return { successCount, errorCount };
  } catch (error) {
    // Gelişmiş hata yakalama
    logger.error("Elasticsearch hatası:");
    if (error.meta) {
      logger.error(`Meta bilgisi: ${JSON.stringify(error.meta.body)}`);
    } else {
      logger.error(error.stack);
    }
    throw error;
  }
};

const calculateAverageAge = (countries) => {
  const validAges = countries
    .map((c) => c.med_age)
    .filter((age) => age > 0 && age < 100);

  return (
    validAges.reduce((sum, age) => sum + age, 0) / validAges.length
  ).toFixed(1);
};

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Uygulama Başlatma
console.clear();
processData();
setInterval(processData, 1800000); // 30 dakikada bir
