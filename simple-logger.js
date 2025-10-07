// simple-logger.js
import fs from 'fs';
import path from 'path';

const LOG_FILE = 'scraper-log.txt';
const MAX_LOG_SIZE = 5 * 1024 * 1024; // 5MB

export const logger = {
  info: (message) => {
    const logMessage = `[INFO] ${new Date().toISOString()} - ${message}\n`;
    writeLog(logMessage);
    console.log(`\x1b[36mℹ️ ${message}\x1b[0m`);
  },
  
  success: (message) => {
    const logMessage = `[SUCCESS] ${new Date().toISOString()} - ${message}\n`;
    writeLog(logMessage);
    console.log(`\x1b[32m✅ ${message}\x1b[0m`);
  },
  
  warn: (message) => {
    const logMessage = `[WARN] ${new Date().toISOString()} - ${message}\n`;
    writeLog(logMessage);
    console.log(`\x1b[33m⚠️ ${message}\x1b[0m`);
  },

  error: (message) => {
    const logMessage = `[ERROR] ${new Date().toISOString()} - ${message}\n`;
    writeLog(logMessage);
    console.log(`\x1b[31m❌ ${message}\x1b[0m`);
  },
  
  data: (operation, count, details = '') => {
    const logMessage = `[DATA] ${new Date().toISOString()} - ${operation}: ${count} records ${details}\n`;
    writeLog(logMessage);
    console.log(`\x1b[35m📊 ${operation}: ${count} records ${details}\x1b[0m`);
  }
};

const writeLog = (message) => {
  try {
    // Log dosyası boyut kontrolü
    if (fs.existsSync(LOG_FILE)) {
      const stats = fs.statSync(LOG_FILE);
      if (stats.size > MAX_LOG_SIZE) {
        // Dosya çok büyükse temizle
        const backupFile = `scraper-log-backup-${Date.now()}.txt`;
        fs.renameSync(LOG_FILE, backupFile);
        console.log(`📁 Log dosyası yedeklendi: ${backupFile}`);
      }
    }
    
    fs.appendFileSync(LOG_FILE, message);
  } catch (error) {
    console.error('Log yazma hatası:', error.message);
  }
};
