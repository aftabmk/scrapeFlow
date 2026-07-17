// workers/exporter-worker.js
const BaseWorker = require('./base-worker');

class ExporterWorker extends BaseWorker {
  constructor() {
    super({
      type: 'exporter'
    });
  }

  async executeTask(message) {
    const { taskId, payload } = message;
    this.currentTask = taskId;

    try {
      // ✅ Get jobs from payload - handle both structures
      let jobs = null;
      
      if (payload.jobs && Array.isArray(payload.jobs)) {
        jobs = payload.jobs;
      } else if (payload.job && payload.job.jobs && Array.isArray(payload.job.jobs)) {
        jobs = payload.job.jobs;
      } else if (payload.job && !Array.isArray(payload.job)) {
        jobs = [payload.job];
      }

      if (!jobs || jobs.length === 0) {
        console.log(`[${this.getDisplayName()}] ⚠️ No jobs found in payload`);
        this.sendTaskComplete(taskId, { status: 'no_jobs', message: 'No jobs to process' });
        return;
      }

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
      const job = jobData.job || jobData;
      const jobId = jobData.jobId || job.id;
      const event = jobData.event || job.data;
      const index = jobData.index || job.metadata?.index || 1;
      const total = jobData.total || job.metadata?.total || jobs.length;

      console.log(`[${this.getDisplayName()}] 📤 Exporting: ${jobId} (${index}/${total})`);

      await this.sleep(100 + Math.random() * 200);

      const exported = {
        jobId: jobId,
        exchange: event?.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
        contract: event?.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
        exported: true,
        exportedAt: new Date().toISOString(),
        summary: {
          hasAnalysis: !!job.data?.analyzed,
          hasScraped: !!job.data?.scraped
        }
      };

      results.push({
        jobId,
        job: {
          ...job,
          data: {
            ...job.data,
            exported
          }
        },
        event: event || job.data,
        index,
        total
      });

      console.log(`[${this.getDisplayName()}] ✅ Exported: ${jobId}`);
    }

    this.processed += results.length;
    console.log(`[${this.getDisplayName()}] ✅ Completed ${results.length} jobs`);

    // Complete batch (final stage)
    this.completeBatch(taskId, jobs, results);
  }

  async processSingle(taskId, jobData) {
    const job = jobData.job || jobData;
    const jobId = jobData.jobId || job.id;
    const event = jobData.event || job.data;

    await this.sleep(100 + Math.random() * 200);

    const exported = {
      jobId: jobId,
      exchange: event?.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
      contract: event?.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
      exported: true,
      exportedAt: new Date().toISOString(),
      summary: {
        hasAnalysis: !!job.data?.analyzed,
        hasScraped: !!job.data?.scraped
      }
    };

    this.processed++;
    console.log(`[${this.getDisplayName()}] ✅ Completed: ${jobId}`);

    // Complete single job (final stage)
    this.completeJob(taskId, job, { exported });
  }
}

if (require.main === module) new ExporterWorker();
module.exports = ExporterWorker;