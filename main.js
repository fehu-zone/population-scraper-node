process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
import dotenv from "dotenv";
dotenv.config();

import { client, initIndex, updateCurrentSnapshot, sendToElasticsearch } from "./elastic/client.js";
import { fetchCountryDataDynamic } from "./scraper/countryDataDynamic.js";
import { fetchWorldDataDynamic } from "./scraper/worldDataDynamic.js";
import { logger } from "./simple-logger.js";

const indexName = "world_population_data";

// ✅ YARDIMCI FONKSİYONLAR
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ✅ VERİ İŞLEME FONKSİYONU
const processData = async () => {
  const processStartTime = new Date();
  const processNumber = Math.floor(Math.random() * 1000);
  
  logger.info(`Veri çekme işlemi #${processNumber} başlatıldı`);

  try {
    // 1. ADIM: Dünya verilerini çek
    logger.info("Dünya verileri çekiliyor");
    const worldData = await fetchWorldDataDynamic();
    
    if (worldData) {
      logger.success(`Dünya nüfusu: ${worldData.current_population?.toLocaleString()}`);
    }

    // 2. ADIM: 10 saniye bekle
    logger.info("10 saniye bekleniyor");
    await delay(10000);
    
    // 3. ADIM: Ülke verilerini çek
    logger.info("Ülke verileri çekiliyor");
    const countryData = await fetchCountryDataDynamic();
    
    if (countryData) {
      logger.success(`Toplam ülke sayısı: ${countryData.length}`);
    }

    // 4. ADIM: Elasticsearch'e gönder
    logger.info("Veriler Elasticsearch'e gönderiliyor");
    const result = await sendToElasticsearch(worldData, countryData);
    
    if (result.error > 0) {
      logger.warn(`Hatalı kayıt: ${result.error}`);
    }

    // 5. ADIM: Snapshot güncelle
    logger.info("Snapshot güncelleniyor");
    await updateCurrentSnapshot(new Date().toISOString());
    
    const processEndTime = new Date();
    const processDuration = (processEndTime - processStartTime) / 1000;
    
    logger.success(`Veri çekme işlemi #${processNumber} tamamlandı - Süre: ${processDuration}s`);
    logger.info(`Bir sonraki veri çekme işlemi 4 dakika sonra`);

  } catch (error) {
    logger.error(`İşlem #${processNumber} hatası: ${error.message}`);
    logger.info("Bir sonraki periyotta tekrar denenecek");
  }
};

// ✅ UYGULAMA BAŞLATMA
const main = async () => {
  console.clear();
  logger.info("Uygulama başlatılıyor");
  
  try {
    // Index hazırlama
    await initIndex();
    
    // İlk veri çekme işlemini başlat
    logger.info("İlk veri çekme işlemi başlatılıyor");
    await processData();
    
    // 4 DAKİKADA BİR tekrarla
    const intervalMinutes = 4;
    const intervalMs = intervalMinutes * 60 * 1000;
    
    logger.success(`Uygulama başlatıldı - Her ${intervalMinutes} dakikada bir veri çekilecek`);
    
    setInterval(async () => {
      logger.info(`${intervalMinutes} dakika tamamlandı, yeni veri çekme işlemi başlatılıyor`);
      await processData();
    }, intervalMs);
    
  } catch (error) {
    logger.error(`Başlatma hatası: ${error.message}`);
    logger.info("15 saniye sonra tekrar denenecek");
    setTimeout(main, 15000);
  }
};

// ✅ PROGRAMI BAŞLAT
main();