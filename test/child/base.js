// child/base.js
const { EventEmitter } = require('events');
const DurableQueue = require('../queue/durable-queue');

class BaseChildProcess extends EventEmitter {
  constructor(options = {}) {
    super();
    
    const args = this._parseArgs();
    
    this.processType = options.processType || args.processType || 'generic';
    this.queueName = options.queueName || args.queueName || `${this.processType}_queue`;
    this.processingWorkers = options.processingWorkers || parseInt(args.processingWorkers) || 2;
    this.dbPath = options.dbPath || './data/queue.db';
    this.isRunning = true;
    
    // Only create queue if not job-submitter (or if processingWorkers > 0)
    if (this.processingWorkers > 0) {
      this.queue = new DurableQueue({
        queueName: this.queueName,
        dbPath: this.dbPath
      });
    } else {
      this.queue = null;
    }
    
    this.activeJobs = new Map();
    this.jobCounter = 0;
    
    console.log(`[${this.processType}] Starting with ${this.processingWorkers} workers`);
    console.log(`[${this.processType}] Queue: ${this.queueName}`);
    
    this._setupIPCListener();
    this._startHeartbeat();
    
    // Only start workers if there are workers to start
    if (this.processingWorkers > 0) {
      this._startWorkers();
    } else {
      console.log(`[${this.processType}] No workers configured (submitter mode)`);
    }
    
    process.send({
      type: 'ready',
      processType: this.processType,
      processingWorkers: this.processingWorkers,
      queueName: this.queueName,
      pid: process.pid
    });
  }

  _parseArgs() {
    const args = {};
    for (let i = 2; i < process.argv.length; i++) {
      const arg = process.argv[i];
      if (arg.startsWith('--')) {
        const [key, value] = arg.slice(2).split('=');
        args[key] = value || true;
      }
    }
    return args;
  }

  _setupIPCListener() {
    process.on('message', async (message) => {
      if (!message || !message.type) return;

      switch (message.type) {
        case 'NEW_JOB':
          await this._handleNewJob(message);
          break;
        case 'GET_STATUS':
          this._sendStatus();
          break;
        case 'SHUTDOWN':
          await this.shutdown();
          break;
        default:
          // Ignore other messages (like START_SUBMITTING for job-submitter)
          // Job-submitter handles this in its own override
      }
    });
  }

  async _handleNewJob(message) {
    if (!this.queue) {
      console.log(`[${this.processType}] No queue available (submitter mode)`);
      return;
    }
    
    try {
      const jobId = await this.queue.enqueue({
        ...message.data,
        jobId: message.jobId
      });
      
      console.log(`[${this.processType}] 📝 Job ${jobId} enqueued to durable queue`);
      
      process.send({
        type: 'JOB_QUEUED',
        jobId,
        timestamp: Date.now()
      });
    } catch (error) {
      console.error(`[${this.processType}] Failed to enqueue job:`, error);
      process.send({
        type: 'JOB_ERROR',
        jobId: message.jobId,
        error: error.message
      });
    }
  }

  _startWorkers() {
    for (let i = 0; i < this.processingWorkers; i++) {
      this._workerLoop(i);
    }
  }

  async _workerLoop(workerId) {
    while (this.isRunning) {
      try {
        if (!this.queue) {
          await this._sleep(1000);
          continue;
        }
        
        const job = await this.queue.dequeue(`worker_${workerId}`);
        
        if (!job) {
          await this._sleep(500);
          continue;
        }

        this.activeJobs.set(job.job_id, { workerId, job, startedAt: Date.now() });

        try {
          console.log(`[${this.processType}] 🔄 Worker ${workerId} processing ${job.job_id}`);
          
          const result = await this._processJob(job);
          
          await this.queue.ack(job.job_id, result);
          
          this.activeJobs.delete(job.job_id);
          console.log(`[${this.processType}] ✅ Job ${job.job_id} completed and acknowledged`);
          
          process.send({
            type: 'JOB_COMPLETE',
            jobId: job.job_id,
            result,
            timestamp: Date.now()
          });
          
        } catch (error) {
          this.activeJobs.delete(job.job_id);
          console.error(`[${this.processType}] ❌ Job ${job.job_id} failed:`, error.message);
          
          process.send({
            type: 'JOB_FAILED',
            jobId: job.job_id,
            error: error.message,
            timestamp: Date.now()
          });
        }
      } catch (error) {
        console.error(`[${this.processType}] Worker ${workerId} error:`, error);
        await this._sleep(1000);
      }
    }
  }

  async _processJob(job) {
    // Override in child classes
    await this._sleep(1000);
    return {
      jobId: job.job_id,
      processedAt: new Date().toISOString(),
      data: job.data,
      result: `Processed by ${this.processType}`
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  _sendStatus() {
    process.send({
      type: 'STATUS',
      processType: this.processType,
      activeJobs: this.activeJobs.size,
      queueSize: this.queue ? this.queue.memoryCache?.pending?.length || 0 : 0,
      pid: process.pid
    });
  }

  _startHeartbeat() {
    setInterval(() => {
      if (!this.isRunning) return;
      
      process.send({
        type: 'HEARTBEAT',
        pid: process.pid,
        timestamp: Date.now(),
        stats: {
          activeJobs: this.activeJobs.size,
          workers: this.processingWorkers,
          queueName: this.queueName
        }
      });
    }, 5000);
  }

  async shutdown() {
    console.log(`[${this.processType}] Shutting down...`);
    this.isRunning = false;
    
    if (this.queue) {
      this.queue.close();
    }
    
    process.send({ type: 'SHUTDOWN_COMPLETE' });
    setTimeout(() => process.exit(0), 500);
  }
}

module.exports = BaseChildProcess;