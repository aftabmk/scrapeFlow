// workers/submitter-worker.js
const BaseWorker = require('./base-worker');

class SubmitterWorker extends BaseWorker {
  constructor() {
    super({
      type: 'submitter'
    });
    
    this.submittedJobs = new Set();
    this.jobStatus = new Map();
    this.submitInterval = parseInt(process.env.SUBMIT_INTERVAL) || 1000;
    this.maxJobs = parseInt(process.env.MAX_JOBS) || 50;
    this.parallelJobs = parseInt(process.env.PARALLEL_JOBS) || 10;
    this.events = [];
    this.currentIndex = 0;
    this.isSubmitting = false;
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
      this.sendTaskComplete(taskId, result);

    } catch (error) {
      this.errors++;
      console.error(`[${this.getDisplayName()}] ❌ Error:`, error.message);
      this.sendTaskFailed(taskId, error);
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

    console.log(`[${this.getDisplayName()}] 🚀 Starting submission of ${this.maxJobs} jobs`);

    const allResults = await this.submitAllJobs();

    console.log(`[${this.getDisplayName()}] ✅ Submission complete: ${allResults.length} jobs`);

    // Send all jobs in one batch to analyzer
    if (allResults.length > 0) {
      console.log(`[${this.getDisplayName()}] 📤 Sending ${allResults.length} jobs to Analyzer`);
      this.routeBatch(this.currentTask, allResults, 'submitter', 'analyzer');
    }

    return {
      status: 'complete',
      totalJobs: allResults.length,
      failedJobs: 0
    };
  }

  async submitAllJobs() {
    if (this.events.length === 0) return [];

    const totalToSubmit = Math.min(this.maxJobs, this.events.length);
    const allResults = [];
    let submitted = 0;

    while (submitted < totalToSubmit && this.isSubmitting) {
      const batchSize = Math.min(this.parallelJobs, totalToSubmit - submitted);
      const batch = [];

      for (let i = 0; i < batchSize; i++) {
        if (this.currentIndex >= this.events.length) break;
        const event = this.events[this.currentIndex];
        if (event && event.EXCHANGE && event.CONTRACT) {
          batch.push({
            event,
            index: this.currentIndex + 1,
            total: totalToSubmit
          });
        }
        this.currentIndex++;
      }

      if (batch.length === 0) break;

      for (const item of batch) {
        const jobId = `${item.event.EXCHANGE}-${item.event.CONTRACT}`;
        const jobData = {
          id: jobId,
          data: item.event,
          metadata: {
            exchange: item.event.EXCHANGE,
            contract: item.event.CONTRACT,
            index: item.index,
            total: item.total
          }
        };

        this.submittedJobs.add(jobId);
        this.jobStatus.set(jobId, {
          status: 'submitted',
          submittedAt: Date.now(),
          event: item.event
        });

        console.log(`[${this.getDisplayName()}] 📤 Submitting: ${jobId} (${item.index}/${item.total})`);

        allResults.push({
          jobId,
          job: jobData,
          event: item.event,
          index: item.index,
          total: item.total
        });

        submitted++;
      }

      if (submitted < totalToSubmit && this.isSubmitting) {
        await this.sleep(this.submitInterval);
      }
    }

    return allResults;
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
      metadata: {
        ...job.metadata,
        submittedAt: Date.now(),
        index: index || 1,
        total: total || 1
      }
    };

    this.submittedJobs.add(jobId);
    this.jobStatus.set(jobId, {
      status: 'submitted',
      submittedAt: Date.now(),
      event
    });

    console.log(`[${this.getDisplayName()}] 📤 Submitting: ${jobId} (${index}/${total})`);

    // Single job routing
    this.routeJob(this.currentTask, jobData, 'submitter', 'analyzer', {
      event,
      index,
      total
    });

    return { jobId, status: 'submitted' };
  }
}

if (require.main === module) new SubmitterWorker();
module.exports = SubmitterWorker;