// config.js
const path = require('path');

// ✅ Load environment variables from .env file
try {
  require('dotenv').config();
} catch (err) {
  // dotenv not installed - use process.env directly
  console.warn('⚠️ dotenv not found, using process.env directly');
}

// ✅ Load event config
let eventConfig = [];
try {
  eventConfig = require('./event.json');
} catch (err) {
  console.warn('⚠️ event.json not found, using empty config');
}

// ✅ Environment-based configuration
const config = {
  // Environment
  env: process.env.NODE_ENV || 'development',
  isProduction: process.env.NODE_ENV === 'production',
  isDevelopment: process.env.NODE_ENV === 'development',
  isTest: process.env.NODE_ENV === 'test',
  
  // Application
  app: {
    heartbeatTimeout: parseInt(process.env.HEARTBEAT_TIMEOUT) || 15000,
    restartDelay: parseInt(process.env.RESTART_DELAY) || 2000,
    submitInterval: parseInt(process.env.SUBMIT_INTERVAL) || 3000,
    maxJobs: parseInt(process.env.MAX_JOBS) || 10
  },
  
  // Workers
  workers: {
    analyzer: parseInt(process.env.ANALYZER_WORKERS) || 2,
    browser: parseInt(process.env.BROWSER_WORKERS) || 2,
    exporter: parseInt(process.env.EXPORTER_WORKERS) || 1
  },
  
  // Database
  database: {
    path: process.env.DB_PATH || './data/queue.db',
    walEnabled: process.env.DB_WAL !== 'false',
    syncMode: process.env.DB_SYNC || 'NORMAL'
  },
  
  // Events
  events: eventConfig,
  
  // Logging
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    pretty: process.env.LOG_PRETTY !== 'false'
  }
};

// ✅ Log configuration on startup (only in development)
if (config.isDevelopment) {
  console.log('📋 Configuration loaded:');
  console.log(`  Environment: ${config.env}`);
  console.log(`  Events: ${config.events.length}`);
  console.log(`  Workers: Analyzer=${config.workers.analyzer}, Browser=${config.workers.browser}, Exporter=${config.workers.exporter}`);
  console.log(`  Submit Interval: ${config.app.submitInterval}ms`);
}

module.exports = config;