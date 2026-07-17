// workers/worker-pool.js
const { Worker } = require('worker_threads');
const { EventEmitter } = require('events');
const path = require('path');

class WorkerPool extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      minWorkers: 1,
      maxWorkers: 4,
      loadBalancer: null,
      eventBus: null,
      ...options,
    };
    
    this.workers = new Map();
    
    this.workerTypes = {
      submitter: { script: path.join(__dirname, 'submitter-worker.js'), count: 1 },
      analyzer: { script: path.join(__dirname, 'analyzer-worker.js'), count: 2 },
      browser: { script: path.join(__dirname, 'browser-worker.js'), count: 2 },
      exporter: { script: path.join(__dirname, 'exporter-worker.js'), count: 1 },
    };
    
    this.stats = { created: 0, destroyed: 0, restarted: 0, errors: 0 };
    this._shuttingDown = false;
    this._shutdownComplete = false;
    
    console.log('[WorkerPool] Initialized');
  }

  async start() {
    if (this._shuttingDown || this._shutdownComplete) {
      console.log('[WorkerPool] Cannot start: already shutting down');
      return this;
    }
    
    console.log('[WorkerPool] Starting...');
    for (const [type, config] of Object.entries(this.workerTypes)) {
      for (let i = 0; i < config.count; i++) {
        await this.createWorker(type);
      }
    }
    console.log(`[WorkerPool] Started with ${this.workers.size} workers`);
    return this;
  }

  async createWorker(type) {
    if (this._shuttingDown || this._shutdownComplete) return null;
    
    const config = this.workerTypes[type];
    if (!config) { console.error(`[WorkerPool] Unknown worker type: ${type}`); return null; }
    
    const workerPath = config.script;
    const workerId = `worker_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
    
    try {
      const fs = require('fs');
      if (!fs.existsSync(workerPath)) {
        console.error(`[WorkerPool] Worker script not found: ${workerPath}`);
        return null;
      }
      
      const worker = new Worker(workerPath, { workerData: { type, id: workerId } });
      
      const info = {
        id: workerId,
        worker,
        type,
        status: 'idle',
        created: Date.now(),
        processed: 0,
        errors: 0,
        currentTask: null,
      };
      
      this.workers.set(workerId, info);
      this.stats.created++;
      
      if (this.options.loadBalancer) {
        this.options.loadBalancer.registerWorker(worker, type);
      }
      
      worker.on('message', (message) => this.handleWorkerMessage(workerId, message));
      worker.on('error', (error) => this.handleWorkerError(workerId, error));
      worker.on('exit', (code) => this.handleWorkerExit(workerId, code));
      
      console.log(`[WorkerPool] Created ${type} worker: ${workerId}`);
      return info;
    } catch (error) {
      console.error(`[WorkerPool] Failed to create worker ${type}:`, error.message);
      return null;
    }
  }

  handleWorkerMessage(workerId, message) {
    const info = this.workers.get(workerId);
    if (!info) return;
    
    switch (message.type) {
      case 'task.complete':
        info.processed++;
        info.status = 'idle';
        info.currentTask = null;
        if (this.options.loadBalancer) {
          this.options.loadBalancer.handleWorkerResponse(workerId, { ...message, workerId, type: 'task.complete' });
        }
        break;
        
      case 'task.failed':
        info.errors++;
        info.status = 'idle';
        info.currentTask = null;
        if (this.options.loadBalancer) {
          this.options.loadBalancer.handleWorkerResponse(workerId, { ...message, workerId, type: 'task.failed' });
        }
        break;
        
      case 'worker.ready':
        info.status = 'idle';
        console.log(`[WorkerPool] Worker ${workerId} (${info.type}) ready`);
        break;
        
      case 'submitter.started':
        console.log(`[WorkerPool] Submitter started: ${message.payload.totalJobs} jobs`);
        break;
        
      case 'submitter.complete':
        console.log(`[WorkerPool] Submitter complete: ${message.payload.totalJobs} jobs`);
        break;
        
      case 'job.complete':
        console.log(`[WorkerPool] ✅ Job complete: ${message.payload.jobId}`);
        this.emit('job.complete', message.payload);
        break;
        
      case 'job.failed':
        console.error(`[WorkerPool] ❌ Job failed: ${message.payload.jobId}`);
        this.emit('job.failed', message.payload);
        break;
        
      case 'worker.shutdown':
        console.log(`[WorkerPool] Worker ${workerId} shutting down`);
        this.workers.delete(workerId);
        break;
        
      default:
        if (this.options.loadBalancer) {
          this.options.loadBalancer.handleWorkerResponse(workerId, message);
        }
    }
  }

  handleWorkerError(workerId, error) {
    const info = this.workers.get(workerId);
    if (!info) return;
    
    this.stats.errors++;
    console.error(`[WorkerPool] Worker ${workerId} (${info.type}) error:`, error.message);
    this.emit('worker.error', { workerId, error: error.message });
    
    if (!this._shuttingDown) {
      this.restartWorker(workerId);
    }
  }

  handleWorkerExit(workerId, code) {
    const info = this.workers.get(workerId);
    if (!info) return;
    
    console.log(`[WorkerPool] Worker ${workerId} (${info.type}) exited with code ${code}`);
    this.stats.destroyed++;
    this.emit('worker.exited', { workerId, code });
    
    if (this.options.loadBalancer) {
      this.options.loadBalancer.unregisterWorker(workerId);
    }
    
    if (!this._shuttingDown) {
      console.log(`[WorkerPool] Restarting worker ${workerId} (${info.type})`);
      this.restartWorker(workerId);
    } else {
      this.workers.delete(workerId);
    }
  }

  async restartWorker(workerId, type = null) {
    if (this._shuttingDown || this._shutdownComplete) {
      console.log('[WorkerPool] Skipping restart: shutting down');
      return null;
    }
    
    const info = this.workers.get(workerId);
    if (info) {
      type = info.type;
    }
    
    if (!type) {
      console.warn(`[WorkerPool] Cannot restart worker ${workerId}: type unknown`);
      return null;
    }
    
    console.log(`[WorkerPool] Restarting worker ${workerId} (${type})`);
    
    if (this.workers.has(workerId)) {
      const oldInfo = this.workers.get(workerId);
      if (oldInfo && oldInfo.worker) {
        try { await oldInfo.worker.terminate(); } catch (err) {}
      }
      this.workers.delete(workerId);
    }
    
    if (this.options.loadBalancer) {
      this.options.loadBalancer.unregisterWorker(workerId);
    }
    
    this.stats.restarted++;
    await this.sleep(1000);
    
    if (this._shuttingDown || this._shutdownComplete) return null;
    
    const newWorker = await this.createWorker(type);
    if (newWorker) {
      this.emit('worker.restarted', { oldId: workerId, newId: newWorker.id });
      console.log(`[WorkerPool] Worker ${workerId} restarted as ${newWorker.id}`);
    } else {
      console.error(`[WorkerPool] Failed to restart worker ${workerId}`);
      this.emit('worker.restart.failed', { workerId });
    }
    return newWorker;
  }

  getWorker(type) {
    const workers = Array.from(this.workers.values());
    return workers.find(w => w.type === type) || null;
  }

  getStats() {
    const workers = Array.from(this.workers.values());
    const byType = {};
    for (const type of Object.keys(this.workerTypes)) {
      const list = workers.filter(w => w.type === type);
      byType[type] = {
        total: list.length,
        idle: list.filter(w => w.status === 'idle').length,
        busy: list.filter(w => w.status === 'busy').length,
      };
    }
    return { total: workers.length, byType, stats: this.stats };
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  async shutdown() {
    if (this._shuttingDown) {
      console.log('[WorkerPool] Already shutting down...');
      return;
    }
    if (this._shutdownComplete) {
      console.log('[WorkerPool] Shutdown already complete');
      return;
    }
    
    this._shuttingDown = true;
    console.log('[WorkerPool] Shutting down...');
    
    const promises = [];
    for (const [id, info] of this.workers) {
      promises.push(
        info.worker.terminate().catch(err => console.error(`[WorkerPool] Error terminating ${id}:`, err.message))
      );
    }
    await Promise.allSettled(promises);
    this.workers.clear();
    
    this._shutdownComplete = true;
    console.log('[WorkerPool] Shutdown complete');
    this.emit('shutdown');
  }
}

module.exports = WorkerPool;