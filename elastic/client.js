import dotenv from "dotenv";
dotenv.config();
import { Client } from "@elastic/elasticsearch";
import config from "../config/index.js";
import { logger } from "../simple-logger.js";

// ✅ BASİT VE ETKİLİ ELASTICSEARCH CLIENT
export const client = new Client({
  node: process.env.ELASTICSEARCH_HOST || "http://localhost:9200",
  requestTimeout: 60000,
  maxRetries: 3,
  pingTimeout: 10000
});

// ✅ BAĞLANTI TESTİ
export const testConnection = async () => {
  try {
    logger.info("Elasticsearch bağlantısı test ediliyor");
    
    await client.ping();
    const clusterInfo = await client.cluster.health();
    
    logger.success(`Elasticsearch bağlantısı başarılı - Cluster: ${clusterInfo.cluster_name}`);
    
    return {
      success: true,
      cluster_name: clusterInfo.cluster_name,
      status: clusterInfo.status
    };
  } catch (error) {
    logger.error(`Elasticsearch bağlantı hatası: ${error.message}`);
    throw error;
  }
};

// ✅ DÜZELTİLMİŞ INDEX OLUŞTURMA
export const initIndex = async () => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;
    
    logger.info(`Index hazırlanıyor: ${indexName}`);

    await testConnection();
    const exists = await client.indices.exists({ index: indexName });
    
    if (!exists) {
      logger.info("Yeni index oluşturuluyor");
      
      await client.indices.create({
        index: indexName,
        body: {
          mappings: {
            dynamic: "strict",
            properties: {
              // ✅ TEMEL ALANLAR
              rank: { type: "integer" },
              country: { 
                type: "text",
                fields: {
                  keyword: { type: "keyword" }
                }
              },
              country_slug: { type: "keyword" },
              country_code: { type: "keyword" },
              
              // ✅ ÜLKE NÜFUS VERİLERİ
              current_population: { type: "long" },
              yearly_change: { type: "float" },
              net_change: { type: "long" },
              migrants: { type: "long" },
              med_age: { type: "float" },
              population_growth: { type: "float" }, // Fertility rate
              urban_population_percent: { type: "float" },
              world_share_percent: { type: "float" },
              
              // ✅ DÜNYA VERİLERİ - DÜZELTİLMİŞ MAPPING
              // BUGÜN
              births_today: { type: "long" },
              dth1s_today: { type: "long" },
              population_growth_today: { type: "long" }, // absolute_growth'tan
              
              // BU YIL
              births_this_year: { type: "long" },
              dth1s_this_year: { type: "long" },
              population_growth_year: { type: "long" }, // absolute_growth_year'dan
              
              // ✅ SİSTEM ALANLARI
              "@timestamp": { type: "date" },
              is_current: { type: "boolean" },
              type: { type: "keyword" },
              data_source: { type: "keyword" }
            }
          },
          settings: {
            number_of_shards: 1,
            number_of_replicas: 0
          }
        },
      });
      
      logger.success(`Index oluşturuldu: ${indexName}`);
    } else {
      logger.info(`Index zaten mevcut: ${indexName}`);
      
      // ✅ MEVCUT MAPPING'I KONTROL ET
      try {
        const currentMapping = await client.indices.getMapping({ index: indexName });
        logger.info("Mevcut mapping kontrol edildi");
      } catch (mappingError) {
        logger.warn(`Mapping kontrol hatası: ${mappingError.message}`);
      }
    }
    
    return { 
      created: !exists,
      indexName: indexName
    };
    
  } catch (error) {
    logger.error(`Index oluşturma hatası: ${error.message}`);
    throw error;
  }
};

// ✅ VERİ GÖNDERME FONKSİYONU - DÜNYA VERİSİNİ HER ÜLKEYE EKLE
export const sendToElasticsearch = async (worldData, countryData) => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;
    const body = [];
    const timestamp = new Date().toISOString();

    // ✅ DÜNYA VERİLERİNİ HAZIRLA
    let worldDataFields = {};
    if (worldData) {
      const { _debug, ...cleanWorldData } = worldData;

      worldDataFields = {
        births_today: cleanWorldData.births_today,
        dth1s_today: cleanWorldData.dth1s_today,
        population_growth_today: cleanWorldData.population_growth,
        births_this_year: cleanWorldData.births_this_year,
        dth1s_this_year: cleanWorldData.dth1s_this_year,
        population_growth_year: cleanWorldData.absolute_growth,
      };
    }

    // ✅ HER ÜLKE VERİSİNE DÜNYA VERİLERİNİ EKLE
    if (countryData && countryData.length > 0) {
      countryData.forEach(country => {
        const { _debug, _raw_cells, ...cleanCountry } = country;

        body.push(
          { index: { _index: indexName } },
          {
            ...cleanCountry,
            ...worldDataFields, // Dünya verileri her ülkeye ekleniyor
            type: "country",
            is_current: true,
            "@timestamp": timestamp,
            data_source: "worldometers"
          }
        );
      });
    }

    if (body.length === 0) {
      logger.warn("Gönderilecek veri yok");
      return { success: 0, error: 0 };
    }

    // ✅ DEBUG: GÖNDERİLEN VERİYİ LOGLA
    logger.info("Elasticsearch'e gönderilen veri:");
    if (worldData) {
      logger.info(`- Dünya verileri her ülkeye eklendi: Doğum=${worldData.births_today}, Ölüm=${worldData.dth1s_today}`);
    }
    logger.info(`- Ülke: ${countryData?.length || 0} kayıt (her birinde dünya verileri var)`);

    const response = await client.bulk({ refresh: true, body });
    
    let successCount = 0;
    let errorCount = 0;

    if (response.items) {
      response.items.forEach(item => {
        if (item.index && item.index.error) {
          errorCount++;
          logger.warn(`Elasticsearch hatası: ${item.index.error.reason}`);
        } else {
          successCount++;
        }
      });
    }

    logger.data("Elasticsearch'e gönderildi", successCount, `Hatalı: ${errorCount}`);
    
    return { 
      success: successCount, 
      error: errorCount 
    };

  } catch (error) {
    logger.error(`Elasticsearch gönderme hatası: ${error.message}`);
    throw error;
  }
};

// ✅ BASİT SNAPSHOT GÜNCELLEME
export const updateCurrentSnapshot = async (timestamp) => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;
    
    logger.info(`Snapshot güncelleniyor: ${timestamp}`);

    const clearResult = await client.updateByQuery({
      index: indexName,
      conflicts: "proceed",
      refresh: true,
      body: {
        script: { source: "ctx._source.is_current = false", lang: "painless" },
        query: { term: { is_current: true } }
      }
    });

    const updateResult = await client.updateByQuery({
      index: indexName,
      conflicts: "proceed", 
      refresh: true,
      body: {
        script: { source: "ctx._source.is_current = true", lang: "painless" },
        query: {
          bool: {
            must: [
              { range: { "@timestamp": { gte: timestamp, lte: timestamp } } },
              { terms: { type: ["world", "country"] } }
            ]
          }
        }
      }
    });

    logger.success(`Snapshot güncellendi: ${updateResult.updated} kayıt`);
    
    return {
      cleared: clearResult.updated,
      updated: updateResult.updated
    };
    
  } catch (error) {
    logger.error(`Snapshot güncelleme hatası: ${error.message}`);
    return { cleared: 0, updated: 0, error: error.message };
  }
};

// ✅ INDEX TEMİZLEME (OPSİYONEL)
export const cleanupOldData = async (daysToKeep = 7) => {
  try {
    const indexName = process.env.INDEX_NAME || config.INDEX_NAME;
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - daysToKeep);
    
    const deleteResult = await client.deleteByQuery({
      index: indexName,
      body: {
        query: {
          range: {
            "@timestamp": {
              lt: cutoffDate.toISOString()
            }
          }
        }
      }
    });
    
    logger.info(`Eski veriler temizlendi: ${deleteResult.deleted} kayıt`);
    return deleteResult;
  } catch (error) {
    logger.error(`Veri temizleme hatası: ${error.message}`);
    return null;
  }
};