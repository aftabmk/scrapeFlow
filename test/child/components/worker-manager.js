// child/components/worker-manager.js
const { EventEmitter } = require('events');

class WorkerManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.workerCount = options.workerCount || 0;
    this.queueManager = options.queueManager;
    this.processJob = options.processJob || (() => {});
    this.onComplete = options.onComplete || (() => {});
    this.onFailed = options.onFailed || (() => {});
    
    this.workers = [];
    this.activeJobs = new Map();
    this.isRunning = false;
  }

  start() {
    if (this.workerCount === 0) {
      console.log('[WorkerManager] ⏳ No workers configured');
      return;
    }

    this.isRunning = true;
    for (let i = 0; i < this.workerCount; i++) {
      this._startWorker(i);
    }
    console.log(`[WorkerManager] ✅ ${this.workerCount} workers started`);
  }

  async _startWorker(workerId) {
    while (this.isRunning) {
      try {
        const job = await this.queueManager.dequeue(`worker_${workerId}`);
        if (!job) {
          await this._sleep(500);
          continue;
        }

        this.activeJobs.set(job.job_id, { workerId, job, startedAt: Date.now() });

        try {
          const result = await this.processJob(job);
          await this.queueManager.ack(job.job_id, result);
          this.activeJobs.delete(job.job_id);
          this.onComplete(job.job_id, result);
        } catch (error) {
          this.activeJobs.delete(job.job_id);
          this.onFailed(job.job_id, error);
        }
      } catch (error) {
        console.error(`[WorkerManager] Worker ${workerId} error:`, error);
        await this._sleep(1000);
      }
    }
  }

  getActiveCount() {
    return this.activeJobs.size;
  }

  getActiveJobs() {
    return Array.from(this.activeJobs.keys());
  }

  stop() {
    this.isRunning = false;
    this.activeJobs.clear();
    console.log('[WorkerManager] ✅ Stopped');
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = WorkerManager;