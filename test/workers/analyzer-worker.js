// workers/analyzer-worker.js
const BaseWorker = require('./base-worker');

class AnalyzerWorker extends BaseWorker {
  constructor() {
    super({
      type: 'analyzer'
    });
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;

    try {
      // ✅ Get jobs from payload - handle both structures
      let jobs = null;
      
      // Case 1: payload.jobs exists (direct array)
      if (payload.jobs && Array.isArray(payload.jobs)) {
        jobs = payload.jobs;
      }
      // Case 2: payload.job.jobs exists (nested)
      else if (payload.job && payload.job.jobs && Array.isArray(payload.job.jobs)) {
        jobs = payload.job.jobs;
      }
      // Case 3: payload.job is a single job
      else if (payload.job && !Array.isArray(payload.job)) {
        // Single job - wrap in array
        jobs = [payload.job];
      }

      if (!jobs || jobs.length === 0) {
        console.log(`[${this.getDisplayName()}] ⚠️ No jobs found in payload`);
        this.sendTaskComplete(taskId, { status: 'no_jobs', message: 'No jobs to process' });
        return;
      }

      // Check if this is a batch
      if (jobs.length > 1) {
        console.log(`[${this.getDisplayName()}] 📦 Received BATCH of ${jobs.length} jobs`);
        await this.processBatch(taskId, jobs);
      } else {
        console.log(`[${this.getDisplayName()}] 📥 Received single job: ${jobs[0].jobId || jobs[0].id}`);
        await this.processSingle(taskId, jobs[0]);
      }

    } catch (error) {
      this.errors++;
      console.error(`[${this.getDisplayName()}] ❌ Error:`, error.message);
      this.sendTaskFailed(taskId, error);
    } finally {
      this.currentTask = null;
    }
  }

  async processBatch(taskId, jobs) {
    const results = [];

    for (const jobData of jobs) {
      // Handle both formats: jobData.job or jobData itself
      const job = jobData.job || jobData;
      const jobId = jobData.jobId || job.id;
      const event = jobData.event || job.data;
      const index = jobData.index || job.metadata?.index || 1;
      const total = jobData.total || job.metadata?.total || jobs.length;

      console.log(`[${this.getDisplayName()}] 🔍 Analyzing: ${jobId} (${index}/${total})`);

      // Simulate analysis work
      await this.sleep(100 + Math.random() * 200);

      const analyzed = {
        jobId: jobId,
        exchange: event?.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
        contract: event?.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
        analyzed: true,
        analyzedAt: new Date().toISOString()
      };

      results.push({
        jobId,
        job: {
          ...job,
          data: {
            ...job.data,
            analyzed
          }
        },
        event: event || job.data,
        index,
        total
      });

      console.log(`[${this.getDisplayName()}] ✅ Analyzed: ${jobId}`);
    }

    this.processed += results.length;
    console.log(`[${this.getDisplayName()}] 📤 Sending ${results.length} jobs to Browser`);
    this.routeBatch(taskId, results, 'analyzer', 'browser');
  }

  async processSingle(taskId, jobData) {
    // Handle both formats
    const job = jobData.job || jobData;
    const jobId = jobData.jobId || job.id;
    const event = jobData.event || job.data;

    await this.sleep(100 + Math.random() * 200);

    const analyzed = {
      jobId: jobId,
      exchange: event?.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
      contract: event?.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
      analyzed: true,
      analyzedAt: new Date().toISOString()
    };

    this.processed++;

    const jobWithAnalysis = {
      ...job,
      data: {
        ...job.data,
        analyzed
      }
    };

    console.log(`[${this.getDisplayName()}] 📤 Sending to Browser: ${jobId}`);
    this.routeJob(taskId, jobWithAnalysis, 'analyzer', 'browser');
  }
}

if (require.main === module) new AnalyzerWorker();
module.exports = AnalyzerWorker;