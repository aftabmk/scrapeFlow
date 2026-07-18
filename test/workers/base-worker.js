// workers/base-worker.js
const { parentPort, workerData } = require('worker_threads');

/**
 * BaseWorker - Abstract base class for all workers
 * Provides common functionality: message handling, task execution, shutdown
 */
class BaseWorker {
  constructor(options = {}) {
    this.id = workerData?.id || options.id || `worker_${Date.now()}`;
    this.type = workerData?.type || options.type || 'base';
    this.isRunning = true;
    this.currentTask = null;
    this.processed = 0;
    this.errors = 0;
    this.startTime = Date.now();
    
    // Send ready message
    this.sendReady();
    
    // Start listening for messages
    this.start();
  }

  /**
   * Get parent port safely
   * Returns null if not in worker thread
   */
  getParentPort() {
    return parentPort || null;
  }

  /**
   * Check if running in worker thread
   */
  isWorkerThread() {
    return !!parentPort;
  }

  /**
   * Send worker ready message to parent
   */
  sendReady() {
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'worker.ready',
        workerId: this.id,
        workerType: this.type,
        timestamp: Date.now()
      });
    }
  }

  /**
   * Start listening for messages
   */
  start() {
    const port = this.getParentPort();
    if (port) {
      port.on('message', async (message) => {
        await this.handleMessage(message);
      });
    }
  }

  /**
   * Handle incoming messages
   * Override this in child classes for custom message handling
   */
  async handleMessage(message) {
    if (!message || !message.type) return;

    // Log received message
    console.log(`[${this.getDisplayName()}] Received: ${message.type}`);

    switch (message.type) {
      case 'execute':
        await this.executeTask(message);
        break;
      case 'shutdown':
        this.shutdown();
        break;
      default:
        console.log(`[${this.getDisplayName()}] Unknown: ${message.type}`);
    }
  }

  /**
   * Execute a task - MUST be overridden by child classes
   */
  async executeTask(message) {
    throw new Error(`[${this.getDisplayName()}] executeTask() must be implemented by child class`);
  }

  /**
   * Send task completion to parent
   */
  sendTaskComplete(taskId, result) {
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'task.complete',
        taskId,
        result,
        workerId: this.id,
        timestamp: Date.now()
      });
    }
  }

  /**
   * Send task failure to parent
   */
  sendTaskFailed(taskId, error) {
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'task.failed',
        taskId,
        error: error.message || error,
        workerId: this.id,
        timestamp: Date.now()
      });
    }
  }

  /**
   * Send job completion to parent
   */
  sendJobComplete(jobId, result) {
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'job.complete',
        payload: {
          jobId,
          result,
          timestamp: Date.now()
        }
      });
    }
  }

  /**
   * Send job failure to parent
   */
  sendJobFailed(jobId, stage, error) {
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'job.failed',
        payload: {
          jobId,
          stage,
          error: error.message || error,
          timestamp: Date.now()
        }
      });
    }
  }

  /**
   * Route job to next stage
   */
  routeJob(taskId, job, from, to, additionalData = {}) {
    this.sendTaskComplete(taskId, {
      jobId: job.id,
      job,
      from,
      to,
      requiresRouting: true,
      nextStage: to,
      currentStage: from,
      timestamp: Date.now(),
      ...additionalData
    });
  }

  /**
   * Route batch of jobs to next stage
   */
  routeBatch(taskId, jobs, from, to) {
    this.sendTaskComplete(taskId, {
      jobs,
      from,
      to,
      requiresRouting: true,
      nextStage: to,
      currentStage: from,
      timestamp: Date.now(),
      isBatch: true
    });
  }

  /**
   * Complete a job (final stage)
   */
  completeJob(taskId, job, result) {
    this.sendTaskComplete(taskId, {
      jobId: job.id,
      job: {
        ...job,
        data: {
          ...job.data,
          ...result
        }
      },
      complete: true,
      timestamp: Date.now()
    });
    
    // Also send job.complete notification
    this.sendJobComplete(job.id, result);
  }

  /**
   * Complete a batch of jobs (final stage)
   */
  completeBatch(taskId, jobs, results) {
    const completedJobs = jobs.map((job, index) => ({
      jobId: job.id,
      job: {
        ...job,
        data: {
          ...job.data,
          ...results[index]
        }
      }
    }));

    this.sendTaskComplete(taskId, {
      jobs: completedJobs,
      complete: true,
      isBatch: true,
      timestamp: Date.now()
    });

    // Send job.complete for each job
    for (const result of results) {
      this.sendJobComplete(result.jobId, result);
    }
  }

  /**
   * Sleep helper
   */
  sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get display name for logging
   */
  getDisplayName() {
    return `${this.type.charAt(0).toUpperCase() + this.type.slice(1)} ${this.id}`;
  }

  /**
   * Shutdown worker
   */
  shutdown() {
    console.log(`[${this.getDisplayName()}] Shutting down...`);
    this.isRunning = false;
    const port = this.getParentPort();
    if (port) {
      port.postMessage({
        type: 'worker.shutdown',
        workerId: this.id,
        timestamp: Date.now()
      });
    }
  }
}

module.exports = BaseWorker;