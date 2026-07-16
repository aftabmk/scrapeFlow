// workers/exporter-worker.js
const { parentPort, workerData } = require('worker_threads');
const fs = require('fs');
const path = require('path');

class ExporterWorker {
  constructor() {
    this.id = workerData.id || `exporter_${Date.now()}`;
    this.type = 'exporter';
    this.isRunning = true;
    this.currentTask = null;
    this.processed = 0;
    this.errors = 0;
    this.startTime = Date.now();
    this.processedJobs = new Map();
    this.exportCache = new Map();
    this.stats = { totalExported: 0, cacheHits: 0, cacheMisses: 0, avgExportTime: 0, totalExportTime: 0, exports: { file: 0, database: 0, api: 0, cache: 0 }, errors: { file: 0, database: 0, api: 0, cache: 0 } };
    this.exportDir = process.env.EXPORT_DIR || './exports';
    this.ensureExportDir();
    this.sendReady();
    this.start();
  }

  sendReady() {
    if (parentPort) {
      parentPort.postMessage({ type: 'worker.ready', workerId: this.id, workerType: this.type, timestamp: Date.now() });
    }
  }

  start() {
    if (parentPort) {
      parentPort.on('message', async (message) => { await this.handleMessage(message); });
    }
  }

  ensureExportDir() {
    if (!fs.existsSync(this.exportDir)) {
      fs.mkdirSync(this.exportDir, { recursive: true });
    }
  }

  async handleMessage(message) {
    if (!message || !message.type) return;
    switch (message.type) {
      case 'execute': await this.executeTask(message); break;
      case 'shutdown': this.shutdown(); break;
      default: console.log(`[Exporter ${this.id}] Unknown message type: ${message.type}`);
    }
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;
    
    try {
      const { job } = payload;
      console.log(`[Exporter ${this.id}] 📤 Exporting: ${job.id}`);
      
      const exported = await this.export(job);
      this.processed++;
      this.stats.totalExported++;
      
      if (parentPort) {
        parentPort.postMessage({
          type: 'task.complete',
          taskId,
          result: { jobId: job.id, job: { ...job, data: { ...job.data, exported } }, complete: true, timestamp: Date.now() },
          workerId: this.id,
          timestamp: Date.now(),
        });
        parentPort.postMessage({
          type: 'job.complete',
          payload: { jobId: job.id, result: exported, timestamp: Date.now() },
        });
      }
    } catch (error) {
      this.errors++;
      console.error(`[Exporter ${this.id}] ❌ Export failed:`, error);
      if (parentPort) {
        parentPort.postMessage({ type: 'task.failed', taskId, error: error.message, workerId: this.id, timestamp: Date.now() });
        parentPort.postMessage({ type: 'job.failed', payload: { jobId: job?.id || 'unknown', stage: 'exporter', error: error.message, timestamp: Date.now() } });
      }
    } finally {
      this.currentTask = null;
    }
  }

  async export(job) {
    const startTime = Date.now();
    const { event, exchange, contract, analyzed, scraped } = job.data || {};
    const cacheKey = `${exchange}-${contract}-${Date.now()}`;
    
    if (this.exportCache.has(cacheKey)) {
      this.stats.cacheHits++;
      return this.exportCache.get(cacheKey);
    }
    this.stats.cacheMisses++;
    
    const exportData = {
      jobId: job.id,
      exchange: exchange || 'UNKNOWN',
      contract: contract || 'UNKNOWN',
      timestamp: Date.now(),
      event: event || {},
      analysis: analyzed || null,
      scraped: scraped || null,
      metadata: { exportedAt: new Date().toISOString(), version: '1.0', source: 'scrape-flow', workerId: this.id },
      summary: this.generateSummary(job),
    };
    
    const destinations = this.determineDestinations(event);
    const results = [];
    for (const dest of destinations) {
      try {
        const result = await this.exportToDestination(dest, exportData);
        results.push({ destination: dest.type, success: true, result });
        this.stats.exports[dest.type] = (this.stats.exports[dest.type] || 0) + 1;
      } catch (error) {
        results.push({ destination: dest.type, success: false, error: error.message });
        this.stats.errors[dest.type] = (this.stats.errors[dest.type] || 0) + 1;
      }
    }
    
    const exported = { ...exportData, exports: results, success: results.every(r => r.success), duration: Date.now() - startTime };
    this.exportCache.set(cacheKey, exported);
    this.processedJobs.set(job.id, exported);
    this.stats.totalExportTime += Date.now() - startTime;
    this.stats.avgExportTime = this.stats.totalExportTime / this.stats.totalExported;
    return exported;
  }

  determineDestinations(event) {
    const destinations = [{ type: 'file', priority: 0 }];
    if (process.env.DB_EXPORT === 'true') destinations.push({ type: 'database', priority: 1 });
    if (process.env.API_EXPORT_URL) destinations.push({ type: 'api', priority: 2, url: process.env.API_EXPORT_URL });
    destinations.push({ type: 'cache', priority: 3 });
    return destinations.sort((a, b) => a.priority - b.priority);
  }

  async exportToDestination(destination, data) {
    switch (destination.type) {
      case 'file': return this.exportToFile(data);
      case 'database': return this.exportToDatabase(data);
      case 'api': return this.exportToAPI(data, destination.url);
      case 'cache': return this.exportToCache(data);
      default: throw new Error(`Unknown destination: ${destination.type}`);
    }
  }

  async exportToFile(data) {
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const filename = `${data.exchange}-${data.contract}-${timestamp}.json`;
    const filepath = path.join(this.exportDir, filename);
    const dir = path.dirname(filepath);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(filepath, JSON.stringify(data, null, 2));
    return { path: filepath, size: fs.statSync(filepath).size, filename };
  }

  async exportToDatabase(data) { await this.sleep(100); return { inserted: true, id: `db_${data.jobId}`, table: 'exports' }; }

  async exportToAPI(data, url) {
    await this.sleep(150 + Math.random() * 100);
    if (Math.random() < 0.05) throw new Error('API request failed');
    return { status: 200, url: url || 'https://api.example.com/export', response: { success: true, id: data.jobId } };
  }

  async exportToCache(data) { return { cached: true, key: `${data.exchange}-${data.contract}`, ttl: 300000 }; }

  generateSummary(job) {
    const { event, analyzed, scraped } = job.data || {};
    return { exchange: event?.EXCHANGE || 'UNKNOWN', contract: event?.CONTRACT || 'UNKNOWN', hasAnalysis: !!analyzed, hasScraped: !!scraped, analysisType: analyzed?.metadata?.type || 'unknown', dataPoints: scraped?.data ? Object.keys(scraped.data).length : 0, timestamp: Date.now() };
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  shutdown() {
    console.log(`[Exporter ${this.id}] Shutting down...`);
    this.isRunning = false;
    if (parentPort) parentPort.postMessage({ type: 'worker.shutdown', workerId: this.id, timestamp: Date.now() });
  }
}

if (require.main === module) new ExporterWorker();
module.exports = ExporterWorker;