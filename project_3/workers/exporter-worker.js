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
            // ✅ Get jobs from payload
            let jobs = null;
            
            switch (true) {
                case (payload.jobs && Array.isArray(payload.jobs)):
                    jobs = payload.jobs;
                    break;
                    
                case (payload.job && payload.job.jobs && Array.isArray(payload.job.jobs)):
                    jobs = payload.job.jobs;
                    break;
                    
                case (payload.job && !Array.isArray(payload.job)):
                    jobs = [payload.job];
                    break;
                    
                default:
                    jobs = [];
            }

            if (!jobs || jobs.length === 0) {
                console.log(`[${this.getDisplayName()}] ⚠️ No jobs found in payload`);
                this.sendTaskComplete(taskId, { 
                    status: 'no_jobs', 
                    message: 'No jobs to process' 
                });
                return;
            }

            console.log(`[${this.getDisplayName()}] 📦 Received ${jobs.length} jobs from Browser`);

            // ✅ Process each job
            const results = [];
            
            for (const jobData of jobs) {
                const job = jobData.job || jobData;
                const jobId = jobData.jobId || job.id;
                
                console.log(`[${this.getDisplayName()}] 📥 Processing job: ${jobId}`);
                
                // ✅ Log scraped data
                if (job.data && job.data.scraped) {
                    const scraped = job.data.scraped;
                    console.log(`[${this.getDisplayName()}] 🤖 Scraped data for ${jobId}:`);
                    console.log(`  URL: ${scraped.url || 'N/A'}`);
                    console.log(`  Title: ${scraped.metadata?.title || 'N/A'}`);
                    console.log(`  Description: ${scraped.metadata?.description || 'N/A'}`);
                    console.log(`  Success: ${scraped.success ? '✅' : '❌'}`);
                } else {
                    console.log(`[${this.getDisplayName()}] ⚠️ No scraped data for ${jobId}`);
                }
                
                // ✅ Add export data
                const exported = {
                    jobId: jobId,
                    exchange: jobData.event?.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
                    contract: jobData.event?.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
                    exported: true,
                    exportedAt: new Date().toISOString(),
                    originalData: job.data || {},
                    scrapedData: job.data?.scraped || null
                };

                results.push({
                    jobId,
                    job: {
                        ...job,
                        data: {
                            ...job.data,
                            exported: exported
                        }
                    },
                    event: jobData.event || job.data,
                });
                
                console.log(`[${this.getDisplayName()}] ✅ Exported: ${jobId}`);
            }

            this.processed += results.length;
            
            console.log(`[${this.getDisplayName()}] ✅ Completed ${results.length} jobs`);

            // ✅ Complete all jobs
            this.completeBatch(taskId, jobs, results);

        } catch (error) {
            this.errors++;
            console.error(`[${this.getDisplayName()}] ❌ Error:`, error.message);
            this.sendTaskFailed(taskId, error);
        } finally {
            this.currentTask = null;
        }
    }
}

if (require.main === module) new ExporterWorker();
module.exports = ExporterWorker;