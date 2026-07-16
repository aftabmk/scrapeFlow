// workers/browser-worker.js
const { parentPort, workerData } = require('worker_threads');

class BrowserWorker {
  constructor() {
    this.id = workerData.id || `browser_${Date.now()}`;
    this.type = 'browser';
    this.isRunning = true;
    this.currentTask = null;
    this.processed = 0;
    this.errors = 0;
    this.startTime = Date.now();
    this.processedJobs = new Map();
    this.scrapeCache = new Map();
    this.stats = { totalScraped: 0, cacheHits: 0, cacheMisses: 0, avgScrapeTime: 0, totalScrapeTime: 0, requests: 0, responses: 0, timeouts: 0 };
    this.maxRequestsPerSecond = 10;
    this.requestTimestamps = [];
    
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
    this.cacheCleanupInterval = setInterval(() => this.cleanupCache(), 60000);
  }

  async handleMessage(message) {
    if (!message || !message.type) return;
    switch (message.type) {
      case 'execute': await this.executeTask(message); break;
      case 'shutdown': this.shutdown(); break;
      default: console.log(`[Browser ${this.id}] Unknown message type: ${message.type}`);
    }
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;
    
    try {
      const { job } = payload;
      console.log(`[Browser ${this.id}] 🌐 Scraping: ${job.id}`);
      
      const scraped = await this.scrape(job);
      this.processed++;
      this.stats.totalScraped++;
      
      if (parentPort) {
        parentPort.postMessage({
          type: 'task.complete',
          taskId,
          result: {
            jobId: job.id,
            job: { ...job, data: { ...job.data, scraped } },
            from: 'browser',
            to: 'exporter',
            requiresRouting: true,
            nextStage: 'exporter',
            currentStage: 'browser',
            timestamp: Date.now(),
          },
          workerId: this.id,
          timestamp: Date.now(),
        });
      }
    } catch (error) {
      this.errors++;
      console.error(`[Browser ${this.id}] ❌ Scrape failed:`, error);
      if (parentPort) {
        parentPort.postMessage({ type: 'task.failed', taskId, error: error.message, workerId: this.id, timestamp: Date.now() });
      }
    } finally {
      this.currentTask = null;
    }
  }

  async scrape(job) {
    const startTime = Date.now();
    const { event, exchange, contract, pageUrl, apiUrl, apiUrlBuilder, referer, analyzed } = job.data || {};
    const cacheKey = `${exchange}-${contract}-${pageUrl}-${apiUrl}`;
    
    if (this.scrapeCache.has(cacheKey)) {
      this.stats.cacheHits++;
      return this.scrapeCache.get(cacheKey);
    }
    this.stats.cacheMisses++;
    await this.applyRateLimit();
    await this.sleep(200 + Math.random() * 300);
    
    const urls = this.determineUrls(event);
    const results = [];
    for (const url of urls) {
      this.stats.requests++;
      try { const result = await this.performRequest(url, referer); this.stats.responses++; results.push(result); }
      catch (error) { this.stats.timeouts++; results.push({ url, error: error.message }); }
    }
    
    const scraped = {
      jobId: job.id,
      exchange: exchange || 'UNKNOWN',
      contract: contract || 'UNKNOWN',
      urls: urls,
      referer: referer || null,
      data: this.extractData(results, event),
      metadata: { scrapedAt: new Date().toISOString(), requestCount: results.length, successCount: results.filter(r => !r.error).length, failureCount: results.filter(r => r.error).length, duration: Date.now() - startTime },
      responses: results.map(r => ({ url: r.url, status: r.status || (r.error ? 500 : 200), size: r.size || 0, error: r.error || null })),
      analysis: analyzed || null,
    };
    
    this.scrapeCache.set(cacheKey, { ...scraped, _cachedAt: Date.now(), _ttl: 300000 });
    this.processedJobs.set(job.id, scraped);
    this.stats.totalScrapeTime += Date.now() - startTime;
    this.stats.avgScrapeTime = this.stats.totalScrapeTime / this.stats.totalScraped;
    return scraped;
  }

  determineUrls(event) {
    const urls = [];
    if (event.API_URL) urls.push({ url: event.API_URL, type: 'api', method: 'GET', priority: 1 });
    if (event.API_URL_BUILDER) urls.push({ url: event.API_URL_BUILDER, type: 'api_builder', method: 'GET', priority: 0 });
    if (event.PAGE_URL) urls.push({ url: event.PAGE_URL, type: 'page', method: 'GET', priority: 2 });
    return urls.sort((a, b) => a.priority - b.priority);
  }

  async performRequest(url, referer) {
    await this.sleep(100 + Math.random() * 200);
    if (Math.random() < 0.05) throw new Error('Request timeout');
    return { url: url.url, status: 200, size: 1024 + Math.random() * 8192, data: { timestamp: Date.now(), data: { mock: true, value: Math.random() * 100 } } };
  }

  extractData(responses, event) {
    const extracted = { exchange: event.EXCHANGE, contract: event.CONTRACT, timestamp: Date.now(), data: {} };
    for (const response of responses) {
      if (response.data && response.data.data) {
        extracted.data[response.url] = response.data.data;
      }
    }
    return extracted;
  }

  async applyRateLimit() {
    const now = Date.now();
    this.requestTimestamps = this.requestTimestamps.filter(t => now - t < 1000);
    if (this.requestTimestamps.length >= this.maxRequestsPerSecond) {
      const waitTime = 1000 - (now - this.requestTimestamps[0]);
      if (waitTime > 0) await this.sleep(waitTime);
    }
    this.requestTimestamps.push(now);
  }

  cleanupCache() {
    const now = Date.now();
    let removed = 0;
    for (const [key, value] of this.scrapeCache) {
      const ttl = value._ttl || 300000;
      if (now - value._cachedAt > ttl) { this.scrapeCache.delete(key); removed++; }
    }
    if (removed > 0) console.log(`[Browser ${this.id}] Cache cleanup: removed ${removed} entries`);
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  shutdown() {
    console.log(`[Browser ${this.id}] Shutting down...`);
    this.isRunning = false;
    clearInterval(this.cacheCleanupInterval);
    if (parentPort) parentPort.postMessage({ type: 'worker.shutdown', workerId: this.id, timestamp: Date.now() });
  }
}

if (require.main === module) new BrowserWorker();
module.exports = BrowserWorker;