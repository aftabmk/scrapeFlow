// workers/submitter-worker.js
const { parentPort, workerData } = require('worker_threads');

class SubmitterWorker {
  constructor() {
    this.id = workerData.id || `submitter_${Date.now()}`;
    this.type = 'submitter';
    this.isRunning = true;
    this.currentTask = null;
    this.processed = 0;
    this.errors = 0;
    this.startTime = Date.now();
    
    this.submittedJobs = new Set();
    this.completedJobs = new Set();
    this.failedJobs = new Set();
    this.jobStatus = new Map();
    
    this.submitInterval = parseInt(process.env.SUBMIT_INTERVAL) || 1000;
    this.maxJobs = parseInt(process.env.MAX_JOBS) || 50;
    this.parallelJobs = parseInt(process.env.PARALLEL_JOBS) || 10;
    this.events = [];
    this.currentIndex = 0;
    this.isSubmitting = false;
    
    this.sendReady();
    this.start();
  }

  sendReady() {
    if (parentPort) {
      parentPort.postMessage({ 
        type: 'worker.ready', 
        workerId: this.id, 
        workerType: this.type, 
        timestamp: Date.now() 
      });
    }
  }

  start() {
    if (parentPort) {
      parentPort.on('message', async (message) => { 
        await this.handleMessage(message); 
      });
    }
    // ✅ No heartbeat interval
  }

  async handleMessage(message) {
    if (!message || !message.type) return;
    
    switch (message.type) {
      case 'execute':
        await this.executeTask(message);
        break;
      case 'shutdown':
        this.shutdown();
        break;
      default:
        console.log(`[Submitter ${this.id}] Unknown message type: ${message.type}`);
    }
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;
    
    try {
      let result;
      
      switch (payload?.type) {
        case 'start_submitting':
          result = await this.startSubmitting(payload);
          break;
        case 'submit_job':
          result = await this.submitSingleJob(payload);
          break;
        default:
          result = { status: 'ignored', message: `Unknown payload type: ${payload?.type}` };
      }
      
      this.processed++;
      
      if (parentPort) {
        parentPort.postMessage({ 
          type: 'task.complete', 
          taskId, 
          result, 
          workerId: this.id, 
          timestamp: Date.now() 
        });
      }
      
    } catch (error) {
      this.errors++;
      console.error(`[Submitter ${this.id}] Task ${taskId} failed:`, error.message);
      
      if (parentPort) {
        parentPort.postMessage({ 
          type: 'task.failed', 
          taskId, 
          error: error.message, 
          workerId: this.id, 
          timestamp: Date.now() 
        });
      }
    } finally {
      this.currentTask = null;
    }
  }

  async startSubmitting(payload) {
    const { events, maxJobs, interval } = payload || {};
    
    if (this.isSubmitting) {
      return { status: 'already_submitting' };
    }
    
    this.events = events || [];
    this.maxJobs = maxJobs || this.events.length || this.maxJobs;
    this.submitInterval = interval || this.submitInterval;
    this.currentIndex = 0;
    this.isSubmitting = true;
    
    console.log(`[Submitter ${this.id}] 🚀 Starting submission of ${this.maxJobs} jobs`);
    
    if (parentPort) {
      parentPort.postMessage({ 
        type: 'submitter.started', 
        workerId: this.id, 
        payload: { totalJobs: this.maxJobs, timestamp: Date.now() } 
      });
    }
    
    await this.submitAllJobs();
    
    console.log(`[Submitter ${this.id}] ✅ Submission complete: ${this.submittedJobs.size} jobs`);
    
    if (parentPort) {
      parentPort.postMessage({ 
        type: 'submitter.complete', 
        workerId: this.id, 
        payload: { totalJobs: this.submittedJobs.size, failedJobs: this.failedJobs.size, timestamp: Date.now() } 
      });
    }
    
    return { status: 'complete', totalJobs: this.submittedJobs.size, failedJobs: this.failedJobs.size };
  }

  async submitAllJobs() {
    if (this.events.length === 0) return;
    
    const totalToSubmit = Math.min(this.maxJobs, this.events.length);
    let submitted = 0;
    
    while (submitted < totalToSubmit && this.isSubmitting) {
      const batchSize = Math.min(this.parallelJobs, totalToSubmit - submitted);
      const batch = [];
      
      for (let i = 0; i < batchSize; i++) {
        if (this.currentIndex >= this.events.length) break;
        const event = this.events[this.currentIndex];
        if (event && event.EXCHANGE && event.CONTRACT) {
          batch.push({ event, index: this.currentIndex + 1, total: totalToSubmit });
        }
        this.currentIndex++;
      }
      
      if (batch.length === 0) break;
      
      for (const item of batch) {
        try {
          const result = await this.submitSingleJob({
            job: { 
              id: `${item.event.EXCHANGE}-${item.event.CONTRACT}`, 
              data: item.event, 
              metadata: { exchange: item.event.EXCHANGE, contract: item.event.CONTRACT, index: item.index, total: item.total } 
            },
            event: item.event,
            index: item.index,
            total: item.total,
          });
          if (result.status === 'submitted') submitted++;
        } catch (error) {
          this.errors++;
          console.error(`[Submitter ${this.id}] Failed to submit:`, error);
        }
      }
      
      if (submitted < totalToSubmit && this.isSubmitting) {
        await this.sleep(this.submitInterval);
      }
    }
  }

  async submitSingleJob(payload) {
    const { job, event, index, total } = payload;
    const jobId = job.id || `${event.EXCHANGE}-${event.CONTRACT}`;
    
    if (this.submittedJobs.has(jobId)) {
      return { jobId, status: 'duplicate' };
    }
    
    const jobData = { 
      id: jobId, 
      data: job.data || job, 
      metadata: { ...job.metadata, submittedAt: Date.now(), index: index || 1, total: total || 1 } 
    };
    
    this.submittedJobs.add(jobId);
    this.jobStatus.set(jobId, { status: 'submitted', submittedAt: Date.now(), event });
    
    console.log(`[Submitter ${this.id}] 📤 Submitting: ${jobId} (${index}/${total})`);
    
    if (parentPort) {
      parentPort.postMessage({
        type: 'task.complete',
        taskId: this.currentTask,
        result: {
          jobId,
          job: jobData,
          event,
          from: 'submitter',
          to: 'analyzer',
          requiresRouting: true,
          nextStage: 'analyzer',
          currentStage: 'submitter',
          timestamp: Date.now(),
        },
        workerId: this.id,
        timestamp: Date.now(),
      });
    }
    
    return { jobId, status: 'submitted' };
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

  shutdown() {
    console.log(`[Submitter ${this.id}] Shutting down...`);
    this.isRunning = false;
    this.isSubmitting = false;
    if (parentPort) {
      parentPort.postMessage({ type: 'worker.shutdown', workerId: this.id, timestamp: Date.now() });
    }
  }
}

if (require.main === module) new SubmitterWorker();
module.exports = SubmitterWorker;