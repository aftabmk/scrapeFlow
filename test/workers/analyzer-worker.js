// workers/analyzer-worker.js
const { parentPort, workerData } = require('worker_threads');

class AnalyzerWorker {
  constructor() {
    this.id = workerData.id || `analyzer_${Date.now()}`;
    this.type = 'analyzer';
    this.isRunning = true;
    this.currentTask = null;
    this.processed = 0;
    this.errors = 0;
    this.startTime = Date.now();
    this.processedJobs = new Map();
    this.analysisCache = new Map();
    this.stats = { totalAnalyzed: 0, cacheHits: 0, cacheMisses: 0, avgAnalysisTime: 0, totalAnalysisTime: 0 };
    
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

  async handleMessage(message) {
    if (!message || !message.type) return;
    switch (message.type) {
      case 'execute': await this.executeTask(message); break;
      case 'shutdown': this.shutdown(); break;
      default: console.log(`[Analyzer ${this.id}] Unknown message type: ${message.type}`);
    }
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;
    
    try {
      const { job } = payload;
      console.log(`[Analyzer ${this.id}] 🔍 Analyzing: ${job.id}`);
      
      const analyzed = await this.analyze(job);
      this.processed++;
      this.stats.totalAnalyzed++;
      
      if (parentPort) {
        parentPort.postMessage({
          type: 'task.complete',
          taskId,
          result: {
            jobId: job.id,
            job: { ...job, data: { ...job.data, analyzed } },
            from: 'analyzer',
            to: 'browser',
            requiresRouting: true,
            nextStage: 'browser',
            currentStage: 'analyzer',
            timestamp: Date.now(),
          },
          workerId: this.id,
          timestamp: Date.now(),
        });
      }
    } catch (error) {
      this.errors++;
      console.error(`[Analyzer ${this.id}] ❌ Analysis failed:`, error);
      if (parentPort) {
        parentPort.postMessage({ type: 'task.failed', taskId, error: error.message, workerId: this.id, timestamp: Date.now() });
      }
    } finally {
      this.currentTask = null;
    }
  }

  async analyze(job) {
    const startTime = Date.now();
    const { event, exchange, contract, pageUrl, apiUrl, apiUrlBuilder, referer } = job.data || {};
    const cacheKey = `${exchange}-${contract}-${pageUrl}`;
    
    if (this.analysisCache.has(cacheKey)) {
      this.stats.cacheHits++;
      return this.analysisCache.get(cacheKey);
    }
    this.stats.cacheMisses++;
    await this.sleep(100 + Math.random() * 200);
    
    const analysis = {
      jobId: job.id,
      exchange: exchange || 'UNKNOWN',
      contract: contract || 'UNKNOWN',
      pageUrl: pageUrl || null,
      apiUrl: apiUrl || null,
      apiUrlBuilder: apiUrlBuilder || null,
      referer: referer || null,
      metadata: {
        type: this.determineType(event),
        priority: this.determinePriority(event),
        complexity: this.determineComplexity(event),
        estimatedTime: this.estimateTime(event),
        dependencies: this.identifyDependencies(event),
      },
      analysis: {
        valid: true,
        timestamp: new Date().toISOString(),
        checksum: this.generateChecksum(event),
        version: '1.0',
        structure: this.analyzeStructure(event),
        patterns: this.identifyPatterns(event),
        risks: this.identifyRisks(event),
      },
      metrics: { analysisTime: Date.now() - startTime, memoryUsed: process.memoryUsage().heapUsed },
      originalEvent: event,
    };
    
    this.analysisCache.set(cacheKey, analysis);
    this.processedJobs.set(job.id, analysis);
    this.stats.totalAnalysisTime += Date.now() - startTime;
    this.stats.avgAnalysisTime = this.stats.totalAnalysisTime / this.stats.totalAnalyzed;
    return analysis;
  }

  determineType(event) {
    if (event.CONTRACT) {
      switch (event.CONTRACT.toUpperCase()) {
        case 'OPTION': return 'options';
        case 'FUTURE': return 'futures';
        case 'EQUITY': return 'equity';
        default: return 'unknown';
      }
    }
    return 'unknown';
  }

  determinePriority(event) {
    const exchange = event.EXCHANGE?.toUpperCase() || '';
    const contract = event.CONTRACT?.toUpperCase() || '';
    if (exchange === 'NSE' && (contract === 'OPTION' || contract === 'FUTURE')) return 'high';
    if (exchange === 'BSE' && contract === 'OPTION') return 'medium';
    return 'low';
  }

  determineComplexity(event) {
    let score = 0;
    if (event.API_URL_BUILDER) score += 2;
    if (event.API_URL && event.API_URL.includes('v3')) score += 1;
    if (event.CONTRACT === 'OPTION') score += 2;
    if (event.CONTRACT === 'FUTURE') score += 1;
    if (event.EXCHANGE === 'BSE') score += 1;
    if (score >= 4) return 'high';
    if (score >= 2) return 'medium';
    return 'low';
  }

  estimateTime(event) {
    const complexity = this.determineComplexity(event);
    switch (complexity) {
      case 'high': return 2000 + Math.random() * 1000;
      case 'medium': return 1000 + Math.random() * 1000;
      default: return 500 + Math.random() * 500;
    }
  }

  identifyDependencies(event) {
    const deps = [];
    if (event.API_URL_BUILDER) deps.push('api_builder');
    if (event.API_URL) deps.push('api_direct');
    if (event.PAGE_URL) deps.push('page_scrape');
    if (event.CONTRACT === 'OPTION') deps.push('option_chain');
    return deps;
  }

  analyzeStructure(event) {
    return { hasApiUrl: !!event.API_URL, hasApiBuilder: !!event.API_URL_BUILDER, hasPageUrl: !!event.PAGE_URL, hasReferer: !!event.REFERER, exchange: event.EXCHANGE, contract: event.CONTRACT };
  }

  identifyPatterns(event) {
    const patterns = [];
    if (event.API_URL && event.API_URL.includes('nseindia')) patterns.push('nse_api');
    if (event.API_URL && event.API_URL.includes('bseindia')) patterns.push('bse_api');
    if (event.CONTRACT === 'OPTION' && event.API_URL_BUILDER) patterns.push('option_chain_v3');
    return patterns;
  }

  identifyRisks(event) {
    const risks = [];
    if (!event.API_URL && !event.API_URL_BUILDER) risks.push({ type: 'no_api_endpoint', severity: 'high' });
    if (!event.PAGE_URL) risks.push({ type: 'no_page_url', severity: 'medium' });
    if (event.CONTRACT === 'OPTION' && !event.API_URL_BUILDER) risks.push({ type: 'option_missing_builder', severity: 'high' });
    if (event.EXCHANGE === 'BSE' && event.CONTRACT === 'FUTURE') risks.push({ type: 'bse_future_complex', severity: 'medium' });
    return risks;
  }

  generateChecksum(event) {
    const str = JSON.stringify({ exchange: event.EXCHANGE, contract: event.CONTRACT, apiUrl: event.API_URL, pageUrl: event.PAGE_URL });
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash;
    }
    return Math.abs(hash).toString(16);
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  shutdown() {
    console.log(`[Analyzer ${this.id}] Shutting down...`);
    this.isRunning = false;
    if (parentPort) parentPort.postMessage({ type: 'worker.shutdown', workerId: this.id, timestamp: Date.now() });
  }
}

if (require.main === module) new AnalyzerWorker();
module.exports = AnalyzerWorker;