// config.js
const path = require('path');
const os = require('os');

// Load environment variables
require('dotenv').config();

const config = {
  env: process.env.NODE_ENV || 'development',
  isProduction: process.env.NODE_ENV === 'production',
  isDevelopment: process.env.NODE_ENV === 'development',
  
  app: {
    name: 'scrape-flow',
    version: '3.0.0',
    startTimeout: 5000,
    shutdownTimeout: 10000,
  },
  
  workers: {
    min: parseInt(process.env.MIN_WORKERS) || 4,
    max: parseInt(process.env.MAX_WORKERS) || 16,
    scaleUpThreshold: parseInt(process.env.SCALE_UP_THRESHOLD) || 100,
    scaleDownThreshold: parseInt(process.env.SCALE_DOWN_THRESHOLD) || 10,
    scaleInterval: parseInt(process.env.SCALE_INTERVAL) || 5000,
    idleTimeout: parseInt(process.env.WORKER_IDLE_TIMEOUT) || 30000,
    heartbeatInterval: parseInt(process.env.HEARTBEAT_INTERVAL) || 3000,
    heartbeatTimeout: parseInt(process.env.HEARTBEAT_TIMEOUT) || 30000,
  },
  
  queues: {
    visibilityTimeout: parseInt(process.env.VISIBILITY_TIMEOUT) || 30000,
    maxRetries: parseInt(process.env.MAX_RETRIES) || 3,
    batchSize: parseInt(process.env.BATCH_SIZE) || 10,
    maxQueueSize: parseInt(process.env.MAX_QUEUE_SIZE) || 10000,
    sweepInterval: parseInt(process.env.SWEEP_INTERVAL) || 1000,
  },
  
  database: {
    path: process.env.DB_PATH || './data/queue.db',
    walEnabled: process.env.DB_WAL !== 'false',
    syncMode: process.env.DB_SYNC || 'NORMAL',
    readWorkers: parseInt(process.env.READ_WORKERS) || 2,
    writeWorkers: parseInt(process.env.WRITE_WORKERS) || 2,
    batchSize: parseInt(process.env.DB_BATCH_SIZE) || 50,
    cacheSize: parseInt(process.env.DB_CACHE_SIZE) || 2000,
    checkpointInterval: parseInt(process.env.CHECKPOINT_INTERVAL) || 5000,
  },
  
  pipeline: {
    stages: ['submitter', 'analyzer', 'browser', 'exporter'],
    submitInterval: parseInt(process.env.SUBMIT_INTERVAL) || 1000,
    maxJobs: parseInt(process.env.MAX_JOBS) || 50,
    parallelJobs: parseInt(process.env.PARALLEL_JOBS) || 10,
  },
  
  logging: {
    level: process.env.LOG_LEVEL || 'info',
    pretty: process.env.LOG_PRETTY !== 'false',
    outputDir: './logs',
    maxFiles: 10,
    maxSize: '100m',
  },
  
  performance: {
    profile: process.env.PROFILE === 'true',
    metricsInterval: parseInt(process.env.METRICS_INTERVAL) || 10000,
    slowThreshold: parseInt(process.env.SLOW_THRESHOLD) || 1000,
  },
};

// Load event config
try {
  const eventConfig = require('./event.json');
  config.events = eventConfig;
} catch (err) {
  console.warn('⚠️ event.json not found, using empty config');
  config.events = [];
}

// Calculate derived values
config.workers.cpus = os.cpus().length;
config.workers.recommended = Math.max(4, config.workers.cpus - 2);

if (config.isDevelopment) {
  console.log('📋 Configuration loaded:');
  console.log(`  Environment: ${config.env}`);
  console.log(`  Events: ${config.events.length}`);
  console.log(`  Workers: ${config.workers.recommended} (recommended)`);
  console.log(`  Queue Batch Size: ${config.queues.batchSize}`);
  console.log(`  DB Path: ${config.database.path}`);
}

module.exports = config;