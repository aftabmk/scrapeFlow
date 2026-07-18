// utils/metrics.js
const { EventEmitter } = require('events');
const os = require('os');
const fs = require('fs');
const path = require('path');
const logger = require('./logger');

/**
 * Metrics - Performance metrics collection and reporting
 * Tracks system, application, and custom metrics
 */
class Metrics extends EventEmitter {
  constructor(options = {}) {
    super();

    this.options = {
      enabled: options.enabled !== false,
      interval: options.interval || 10000,
      outputDir: options.outputDir || './performance/metrics',
      maxSamples: options.maxSamples || 1000,
      retention: options.retention || 7 * 24 * 60 * 60 * 1000, // 7 days
      ...options,
    };

    // Metric storage
    this.metrics = new Map();
    this.timers = new Map();
    this.counters = new Map();
    this.gauges = new Map();
    this.histograms = new Map();
    this.samples = [];

    // System metrics
    this.systemMetrics = {
      cpu: [],
      memory: [],
      load: [],
      network: [],
    };

    // Application metrics
    this.appMetrics = {
      jobs: {
        total: 0,
        completed: 0,
        failed: 0,
        retried: 0,
        deadLettered: 0,
        inProgress: 0,
        queued: 0,
        processingTime: [],
      },
      workers: {
        total: 0,
        active: 0,
        idle: 0,
        restarts: 0,
      },
      queues: {},
      events: {
        published: 0,
        delivered: 0,
        errors: 0,
      },
    };

    // Start collection
    if (this.options.enabled) {
      this.startCollection();
      this.startReporting();
    }

    logger.info('[Metrics] Initialized', { enabled: this.options.enabled });
  }

  /**
   * Start metrics collection
   */
  startCollection() {
    this.collectInterval = setInterval(() => {
      this.collectSystemMetrics();
      this.collectAppMetrics();
    }, this.options.interval);

    // Start CPU collection more frequently
    this.cpuInterval = setInterval(() => {
      this.collectCPUMetrics();
    }, 2000);

    // Start memory collection more frequently
    this.memoryInterval = setInterval(() => {
      this.collectMemoryMetrics();
    }, 5000);

    // Start event loop monitoring
    this.startEventLoopMonitoring();

    logger.debug('[Metrics] Collection started');
  }

  /**
   * Start event loop monitoring
   */
  startEventLoopMonitoring() {
    let lastCheck = Date.now();
    let lastUsage = process.cpuUsage();

    setInterval(() => {
      const now = Date.now();
      const usage = process.cpuUsage(lastUsage);
      const elapsed = (now - lastCheck) / 1000;

      // CPU usage percentage
      const cpuPercent = {
        user: (usage.user / 1000) / elapsed * 100,
        system: (usage.system / 1000) / elapsed * 100,
        total: ((usage.user + usage.system) / 1000) / elapsed * 100,
      };

      this.recordMetric('system.cpu', 'gauge', cpuPercent);

      // Event loop delay
      const start = process.hrtime.bigint();
      setImmediate(() => {
        const delay = Number(process.hrtime.bigint() - start) / 1e6;
        this.recordMetric('system.eventLoop.delay', 'gauge', delay);
        this.recordMetric('system.eventLoop.delay.max', 'gauge', delay);
      });

      lastCheck = now;
      lastUsage = usage;

    }, 1000);
  }

  /**
   * Collect system metrics
   */
  collectSystemMetrics() {
    const cpus = os.cpus();
    const totalCpu = cpus.reduce((acc, cpu) => {
      const total = Object.values(cpu.times).reduce((s, t) => s + t, 0);
      const idle = cpu.times.idle;
      return acc + ((total - idle) / total * 100);
    }, 0) / cpus.length;

    const memory = process.memoryUsage();
    const systemMemory = os.totalmem();
    const freeMemory = os.freemem();

    const metrics = {
      cpu: {
        total: totalCpu,
        cores: cpus.length,
        perCore: cpus.map((cpu, i) => {
          const total = Object.values(cpu.times).reduce((s, t) => s + t, 0);
          const idle = cpu.times.idle;
          return { core: i, usage: (total - idle) / total * 100 };
        }),
      },
      memory: {
        rss: memory.rss,
        heapUsed: memory.heapUsed,
        heapTotal: memory.heapTotal,
        external: memory.external,
        system: {
          total: systemMemory,
          free: freeMemory,
          used: systemMemory - freeMemory,
          usedPercent: ((systemMemory - freeMemory) / systemMemory) * 100,
        },
      },
      load: {
        average: os.loadavg(),
        uptime: os.uptime(),
      },
      process: {
        pid: process.pid,
        ppid: process.ppid,
        title: process.title,
        versions: process.versions,
        platform: process.platform,
        arch: process.arch,
        uptime: process.uptime(),
      },
    };

    this.recordMetric('system.cpu.total', 'gauge', metrics.cpu.total);
    this.recordMetric('system.memory.rss', 'gauge', metrics.memory.rss / 1024 / 1024);
    this.recordMetric('system.memory.heapUsed', 'gauge', metrics.memory.heapUsed / 1024 / 1024);
    this.recordMetric('system.memory.heapTotal', 'gauge', metrics.memory.heapTotal / 1024 / 1024);
    this.recordMetric('system.load.average', 'gauge', metrics.load.average[0]);

    this.systemMetrics = metrics;
    this.emit('metrics.collected', { system: metrics });
  }

  /**
   * Collect CPU metrics
   */
  collectCPUMetrics() {
    const cpus = os.cpus();
    const totalCpu = cpus.reduce((acc, cpu) => {
      const total = Object.values(cpu.times).reduce((s, t) => s + t, 0);
      const idle = cpu.times.idle;
      return acc + ((total - idle) / total * 100);
    }, 0) / cpus.length;

    this.recordMetric('system.cpu.instant', 'gauge', totalCpu);
    this.systemMetrics.cpu.push({
      timestamp: Date.now(),
      usage: totalCpu,
    });

    // Keep only last 100 samples
    if (this.systemMetrics.cpu.length > 100) {
      this.systemMetrics.cpu.shift();
    }
  }

  /**
   * Collect memory metrics
   */
  collectMemoryMetrics() {
    const memory = process.memoryUsage();
    const systemMemory = os.totalmem();
    const freeMemory = os.freemem();

    const metrics = {
      rss: memory.rss / 1024 / 1024,
      heapUsed: memory.heapUsed / 1024 / 1024,
      heapTotal: memory.heapTotal / 1024 / 1024,
      external: memory.external / 1024 / 1024,
      systemUsed: (systemMemory - freeMemory) / 1024 / 1024,
      systemTotal: systemMemory / 1024 / 1024,
    };

    this.recordMetric('system.memory.rss.mb', 'gauge', metrics.rss);
    this.recordMetric('system.memory.heapUsed.mb', 'gauge', metrics.heapUsed);
    this.recordMetric('system.memory.heapTotal.mb', 'gauge', metrics.heapTotal);
    this.recordMetric('system.memory.external.mb', 'gauge', metrics.external);

    this.systemMetrics.memory.push({
      timestamp: Date.now(),
      ...metrics,
    });

    if (this.systemMetrics.memory.length > 100) {
      this.systemMetrics.memory.shift();
    }
  }

  /**
   * Collect application metrics
   */
  collectAppMetrics() {
    // Jobs
    this.recordMetric('app.jobs.total', 'counter', this.appMetrics.jobs.total);
    this.recordMetric('app.jobs.completed', 'counter', this.appMetrics.jobs.completed);
    this.recordMetric('app.jobs.failed', 'counter', this.appMetrics.jobs.failed);
    this.recordMetric('app.jobs.inProgress', 'gauge', this.appMetrics.jobs.inProgress);
    this.recordMetric('app.jobs.queued', 'gauge', this.appMetrics.jobs.queued);

    // Workers
    this.recordMetric('app.workers.total', 'gauge', this.appMetrics.workers.total);
    this.recordMetric('app.workers.active', 'gauge', this.appMetrics.workers.active);
    this.recordMetric('app.workers.idle', 'gauge', this.appMetrics.workers.idle);

    // Events
    this.recordMetric('app.events.published', 'counter', this.appMetrics.events.published);
    this.recordMetric('app.events.delivered', 'counter', this.appMetrics.events.delivered);
    this.recordMetric('app.events.errors', 'counter', this.appMetrics.events.errors);

    // Event loop
    if (this._eventLoopDelay) {
      this.recordMetric('app.eventLoop.delay', 'gauge', this._eventLoopDelay);
    }

    // Garbage collection
    if (global.gc) {
      const gcStats = process.memoryUsage();
      this.recordMetric('app.gc.heapUsed', 'gauge', gcStats.heapUsed / 1024 / 1024);
      this.recordMetric('app.gc.heapTotal', 'gauge', gcStats.heapTotal / 1024 / 1024);
    }
  }

  /**
   * Start reporting
   */
  startReporting() {
    this.reportInterval = setInterval(() => {
      this.report();
    }, this.options.interval * 2);
  }

  /**
   * Report metrics
   */
  report() {
    if (!this.options.enabled) return;

    const report = {
      timestamp: Date.now(),
      system: this.systemMetrics,
      app: this.appMetrics,
      summary: this.getSummary(),
    };

    // Save report
    this.saveReport(report);

    // Emit report event
    this.emit('metrics.report', report);

    logger.debug('[Metrics] Report generated', { 
      jobs: report.app.jobs.total,
      workers: report.app.workers.total,
    });
  }

  /**
   * Save report to file
   */
  saveReport(report) {
    try {
      const dir = this.options.outputDir;
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
      }

      const filename = `metrics-${new Date().toISOString().split('T')[0]}.json`;
      const filepath = path.join(dir, filename);

      // Read existing data
      let existing = [];
      if (fs.existsSync(filepath)) {
        try {
          const data = fs.readFileSync(filepath, 'utf8');
          existing = JSON.parse(data);
          if (!Array.isArray(existing)) {
            existing = [existing];
          }
        } catch (err) {
          existing = [];
        }
      }

      // Add new report
      existing.push(report);

      // Keep only last 1000 entries
      if (existing.length > 1000) {
        existing = existing.slice(-1000);
      }

      // Write back
      fs.writeFileSync(filepath, JSON.stringify(existing, null, 2));

    } catch (error) {
      logger.error('[Metrics] Failed to save report', error);
    }
  }

  /**
   * Record a metric
   */
  recordMetric(name, type, value, tags = {}) {
    if (!this.options.enabled) return;

    const metric = {
      name,
      type,
      value,
      tags,
      timestamp: Date.now(),
    };

    // Store in appropriate collection
    switch (type) {
      case 'counter':
        if (!this.counters.has(name)) {
          this.counters.set(name, 0);
        }
        this.counters.set(name, this.counters.get(name) + value);
        break;

      case 'gauge':
        this.gauges.set(name, value);
        break;

      case 'histogram':
        if (!this.histograms.has(name)) {
          this.histograms.set(name, []);
        }
        const hist = this.histograms.get(name);
        hist.push(value);
        if (hist.length > 100) {
          hist.shift();
        }
        break;

      case 'timer':
        this.timers.set(name, value);
        break;

      default:
        this.metrics.set(name, value);
    }

    // Store sample
    this.samples.push(metric);
    if (this.samples.length > this.options.maxSamples) {
      this.samples.shift();
    }

    this.emit('metric.recorded', metric);
  }

  /**
   * Start a timer
   */
  startTimer(name) {
    const start = process.hrtime.bigint();
    this.timers.set(`_start_${name}`, start);
    return () => {
      const end = process.hrtime.bigint();
      const startTime = this.timers.get(`_start_${name}`);
      if (startTime) {
        const duration = Number(end - startTime) / 1e6; // milliseconds
        this.recordMetric(name, 'timer', duration);
        this.timers.delete(`_start_${name}`);
        return duration;
      }
      return null;
    };
  }

  /**
   * Time a function
   */
  async time(name, fn) {
    const endTimer = this.startTimer(name);
    try {
      const result = await fn();
      endTimer();
      return result;
    } catch (error) {
      endTimer();
      throw error;
    }
  }

  /**
   * Get metric value
   */
  getMetric(name) {
    if (this.gauges.has(name)) {
      return this.gauges.get(name);
    }
    if (this.counters.has(name)) {
      return this.counters.get(name);
    }
    if (this.metrics.has(name)) {
      return this.metrics.get(name);
    }
    return null;
  }

  /**
   * Get metric summary
   */
  getSummary() {
    const summary = {
      system: {
        cpu: {
          total: this.getMetric('system.cpu.total'),
          cores: os.cpus().length,
        },
        memory: {
          rss: this.getMetric('system.memory.rss.mb'),
          heapUsed: this.getMetric('system.memory.heapUsed.mb'),
          heapTotal: this.getMetric('system.memory.heapTotal.mb'),
        },
        load: os.loadavg()[0],
        uptime: process.uptime(),
      },
      application: {
        jobs: {
          total: this.getMetric('app.jobs.total') || 0,
          completed: this.getMetric('app.jobs.completed') || 0,
          failed: this.getMetric('app.jobs.failed') || 0,
          inProgress: this.getMetric('app.jobs.inProgress') || 0,
          queued: this.getMetric('app.jobs.queued') || 0,
        },
        workers: {
          total: this.getMetric('app.workers.total') || 0,
          active: this.getMetric('app.workers.active') || 0,
          idle: this.getMetric('app.workers.idle') || 0,
        },
        events: {
          published: this.getMetric('app.events.published') || 0,
          delivered: this.getMetric('app.events.delivered') || 0,
          errors: this.getMetric('app.events.errors') || 0,
        },
      },
    };

    return summary;
  }

  /**
   * Get full metrics
   */
  getMetrics() {
    return {
      counters: Object.fromEntries(this.counters),
      gauges: Object.fromEntries(this.gauges),
      timers: Object.fromEntries(this.timers),
      histograms: Object.fromEntries(
        Array.from(this.histograms.entries()).map(([name, values]) => {
          const sorted = [...values].sort((a, b) => a - b);
          const sum = values.reduce((s, v) => s + v, 0);
          return [
            name,
            {
              count: values.length,
              min: sorted[0],
              max: sorted[sorted.length - 1],
              avg: sum / values.length,
              p50: sorted[Math.floor(values.length * 0.5)],
              p90: sorted[Math.floor(values.length * 0.9)],
              p95: sorted[Math.floor(values.length * 0.95)],
              p99: sorted[Math.floor(values.length * 0.99)],
            },
          ];
        })
      ),
      system: this.systemMetrics,
      app: this.appMetrics,
      samples: this.samples.slice(-100),
    };
  }

  /**
   * Increment a counter
   */
  increment(name, value = 1, tags = {}) {
    this.recordMetric(name, 'counter', value, tags);
  }

  /**
   * Set a gauge
   */
  gauge(name, value, tags = {}) {
    this.recordMetric(name, 'gauge', value, tags);
  }

  /**
   * Record a histogram value
   */
  histogram(name, value, tags = {}) {
    this.recordMetric(name, 'histogram', value, tags);
  }

  /**
   * Update job metrics
   */
  updateJobMetrics(stats) {
    Object.assign(this.appMetrics.jobs, stats);
    this.emit('metrics.jobs.updated', stats);
  }

  /**
   * Update worker metrics
   */
  updateWorkerMetrics(stats) {
    Object.assign(this.appMetrics.workers, stats);
    this.emit('metrics.workers.updated', stats);
  }

  /**
   * Update event metrics
   */
  updateEventMetrics(stats) {
    Object.assign(this.appMetrics.events, stats);
    this.emit('metrics.events.updated', stats);
  }

  /**
   * Update queue metrics
   */
  updateQueueMetrics(queueName, stats) {
    if (!this.appMetrics.queues[queueName]) {
      this.appMetrics.queues[queueName] = {};
    }
    Object.assign(this.appMetrics.queues[queueName], stats);
    this.emit('metrics.queue.updated', { queueName, stats });
  }

  /**
   * Get health metrics
   */
  getHealthMetrics() {
    const summary = this.getSummary();
    const memoryPercent = (summary.system.memory.heapUsed / summary.system.memory.heapTotal) * 100;
    const cpuPercent = summary.system.cpu.total;

    return {
      status: this.getHealthStatus(cpuPercent, memoryPercent),
      cpuPercent,
      memoryPercent,
      jobsPending: summary.application.jobs.queued,
      workersActive: summary.application.workers.active,
      workersTotal: summary.application.workers.total,
      errorRate: summary.application.jobs.failed / (summary.application.jobs.completed + summary.application.jobs.failed) || 0,
    };
  }

  /**
   * Get health status
   */
  getHealthStatus(cpuPercent, memoryPercent) {
    if (cpuPercent > 90 || memoryPercent > 90) {
      return 'critical';
    }
    if (cpuPercent > 70 || memoryPercent > 80) {
      return 'warning';
    }
    if (cpuPercent > 50 || memoryPercent > 70) {
      return 'degraded';
    }
    return 'healthy';
  }

  /**
   * Export metrics
   */
  exportMetrics() {
    return {
      timestamp: Date.now(),
      metrics: this.getMetrics(),
      summary: this.getSummary(),
      health: this.getHealthMetrics(),
    };
  }

  /**
   * Reset metrics
   */
  reset() {
    this.metrics.clear();
    this.timers.clear();
    this.counters.clear();
    this.gauges.clear();
    this.histograms.clear();
    this.samples = [];
    this.systemMetrics = {
      cpu: [],
      memory: [],
      load: [],
      network: [],
    };
    this.appMetrics = {
      jobs: {
        total: 0,
        completed: 0,
        failed: 0,
        retried: 0,
        deadLettered: 0,
        inProgress: 0,
        queued: 0,
        processingTime: [],
      },
      workers: {
        total: 0,
        active: 0,
        idle: 0,
        restarts: 0,
      },
      queues: {},
      events: {
        published: 0,
        delivered: 0,
        errors: 0,
      },
    };

    this.emit('metrics.reset');
    logger.info('[Metrics] Reset');
  }

  /**
   * Shutdown
   */
  shutdown() {
    logger.info('[Metrics] Shutting down...');

    clearInterval(this.collectInterval);
    clearInterval(this.cpuInterval);
    clearInterval(this.memoryInterval);
    clearInterval(this.reportInterval);

    // Final report
    this.report();

    this.removeAllListeners();
    logger.info('[Metrics] Shutdown complete');
  }
}

// Create singleton instance
const metrics = new Metrics({
  enabled: process.env.PROFILE === 'true' || false,
  interval: parseInt(process.env.METRICS_INTERVAL) || 10000,
  outputDir: process.env.METRICS_OUTPUT_DIR || './performance/metrics',
  maxSamples: 1000,
});

module.exports = metrics;