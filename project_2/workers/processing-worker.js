// workers/processing-worker.js
const { EventEmitter } = require('events');

class ProcessingWorker extends EventEmitter {
  constructor(options = {}) {
    super();
    this.workerId = options.workerId || `worker_${Date.now()}`;
    this.sqliteComm = options.sqliteComm;
    this.handler = options.handler;
    this.isBusy = false;
    this.currentJob = null;
    this.isRunning = true;
    this.pollInterval = options.pollInterval || 1000;
    this.batchSize = options.batchSize || 1;
    
    this._startProcessing();
  }

  async _startProcessing() {
    while (this.isRunning) {
      try {
        await this._processNextJob();
      } catch (error) {
        console.error(`Worker ${this.workerId} error:`, error);
        await this._sleep(this.pollInterval);
      }
    }
  }

  async _processNextJob() {
    if (this.isBusy) {
      await this._sleep(100);
      return;
    }

    let job;
    if (this.batchSize > 1) {
      const jobs = await this.sqliteComm.dequeueMultiple(this.workerId, this.batchSize);
      if (jobs.length === 0) {
        await this._sleep(this.pollInterval);
        return;
      }
      job = jobs[0];
    } else {
      job = await this.sqliteComm.dequeue(this.workerId);
      if (!job) {
        await this._sleep(this.pollInterval);
        return;
      }
    }

    this.isBusy = true;
    this.currentJob = job;

    try {
      this.emit('jobStarted', { workerId: this.workerId, jobId: job.job_id });
      
      const result = await this.handler(job);
      await this.sqliteComm.ack(job.job_id, result);
      
      this.emit('jobComplete', { workerId: this.workerId, jobId: job.job_id, result });
      
    } 
    catch (error) {
      console.error(`Worker ${this.workerId} failed job ${job.job_id}:`, error);
      this.emit('jobFailed', { workerId: this.workerId, jobId: job.job_id, error });
      
    } finally {
      this.isBusy = false;
      this.currentJob = null;
    }
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  getStatus() {
    return {
      workerId: this.workerId,
      isBusy: this.isBusy,
      currentJob: this.currentJob,
      isRunning: this.isRunning
    };
  }

  shutdown() {
    this.isRunning = false;
    this.emit('shutdown', { workerId: this.workerId });
  }

  forceStop() {
    this.isRunning = false;
    this.isBusy = false;
    this.currentJob = null;
    this.emit('stopped', { workerId: this.workerId });
  }
}

module.exports = ProcessingWorker;