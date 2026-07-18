// workers/browser-worker.js
const BaseWorker = require('./base-worker');

class BrowserWorker extends BaseWorker {
    constructor() {
        super({
            type: 'browser'
        });
        
        this.puppeteerReady = false;
        this.pendingRequests = new Map();
        this.isProcessing = false;
        this.currentBatchId = null;
        this.processedJobs = new Set(); // ✅ Track processed job IDs to prevent duplicates
        
        this.setupListeners();
    }

    setupListeners() {
        const port = this.getParentPort();
        if (!this.isWorkerThread()) {
            console.log(`[${this.getDisplayName()}] Not in worker thread`);
            return;
        }
        
        port.on('message', (message) => {
            switch (message.type) {
                case 'PUPPETEER_READY':
                    console.log(`[${this.getDisplayName()}] ✅ Puppeteer ready`);
                    this.puppeteerReady = true;
                    break;
                    
                case 'SCRAPE_RESPONSE':
                    console.log(`[${this.getDisplayName()}] 📥 Received SCRAPE_RESPONSE`);
                    this.handleScrapeResponse(message);
                    break;
                    
                case 'SQLITE_READY':
                    // Silently ignore
                    break;
                    
                default:
                    // Ignore other messages
                    break;
            }
        });
    }

    handleScrapeResponse(message) {
        const { messageId, payload } = message;
        
        console.log(`[${this.getDisplayName()}] 💬 Received response for: ${messageId}`);
        
        const pending = this.pendingRequests.get(messageId);
        if (pending) {
            if (payload.duplicate) {
                console.log(`[${this.getDisplayName()}] ⚠️ Duplicate request detected, resolving with existing data`);
                pending.resolve({ success: false, duplicate: true, message: 'Already processing' });
            } else {
                pending.resolve(payload);
            }
            this.pendingRequests.delete(messageId);
        } else {
            console.warn(`[${this.getDisplayName()}] ⚠️ No pending request for: ${messageId}`);
        }
    }

    waitForPuppeteer() {
        return new Promise((resolve) => {
            if (this.puppeteerReady) {
                resolve();
                return;
            }
            
            const port = this.getParentPort();
            if (!port) {
                resolve();
                return;
            }
            
            const listener = (message) => {
                if (message.type === 'PUPPETEER_READY') {
                    this.puppeteerReady = true;
                    port.removeListener('message', listener);
                    resolve();
                }
            };
            
            port.on('message', listener);
            
            setTimeout(() => {
                port.removeListener('message', listener);
                console.log(`[${this.getDisplayName()}] ⚠️ Puppeteer timeout, continuing...`);
                resolve();
            }, 5000);
        });
    }

    async executeTask(message) {
        const { taskId, payload } = message;
        
        // ✅ Prevent concurrent processing
        if (this.isProcessing) {
            console.log(`[${this.getDisplayName()}] ⚠️ Already processing, ignoring duplicate`);
            return;
        }
        
        this.isProcessing = true;
        this.currentTask = taskId;
        this.currentBatchId = `batch_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;

        try {
            // Wait for puppeteer ready
            if (!this.puppeteerReady) {
                console.log(`[${this.getDisplayName()}] ⏳ Waiting for puppeteer...`);
                await this.waitForPuppeteer();
            }

            // Get jobs from payload
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
                console.log(`[${this.getDisplayName()}] ⚠️ No jobs found`);
                this.sendTaskComplete(taskId, { 
                    status: 'no_jobs', 
                    message: 'No jobs to process' 
                });
                this.isProcessing = false;
                return;
            }

            // ✅ Filter out already processed jobs
            const uniqueJobs = jobs.filter(jobData => {
                const jobId = jobData.jobId || jobData.job?.id || jobData.id;
                if (this.processedJobs.has(jobId)) {
                    console.log(`[${this.getDisplayName()}] ⚠️ Job ${jobId} already processed, skipping`);
                    return false;
                }
                return true;
            });

            if (uniqueJobs.length === 0) {
                console.log(`[${this.getDisplayName()}] ⚠️ All jobs already processed`);
                this.sendTaskComplete(taskId, { 
                    status: 'already_processed', 
                    message: 'All jobs already processed' 
                });
                this.isProcessing = false;
                return;
            }

            console.log(`[${this.getDisplayName()}] 📦 Received ${uniqueJobs.length} unique jobs (filtered from ${jobs.length})`);

            // ✅ Send scrape requests concurrently
            const scrapePromises = uniqueJobs.map((jobData) => {
                const job = jobData.job || jobData;
                const event = jobData.event || job.data;
                const jobId = jobData.jobId || job.id;
                
                // ✅ Mark as processed
                this.processedJobs.add(jobId);
                
                const scrapePayload = {
                    jobId: jobId,
                    url: event.PAGE_URL || job.data?.PAGE_URL,
                    exchange: event.EXCHANGE || job.data?.EXCHANGE || 'UNKNOWN',
                    contract: event.CONTRACT || job.data?.CONTRACT || 'UNKNOWN',
                    eventData: event || job.data,
                    analysis: job.data?.analyzed || null,
                };
                
                return new Promise((resolve, reject) => {
                    const messageId = `scrape_${jobId}_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
                    
                    this.pendingRequests.set(messageId, { resolve, reject });
                    
                    const port = this.getParentPort();
                    if (port) {
                        port.postMessage({
                            type: 'SCRAPE_REQUEST',
                            messageId: messageId,
                            sourceWorkerId: this.id,
                            payload: scrapePayload,
                            batchId: this.currentBatchId
                        });
                        console.log(`[${this.getDisplayName()}] 📤 Sent SCRAPE_REQUEST for ${jobId} (${messageId})`);
                    } else {
                        reject(new Error('No parentPort available'));
                    }
                });
            });

            console.log(`[${this.getDisplayName()}] ⏳ Waiting for ${scrapePromises.length} responses...`);
            
            // ✅ Wait for all responses with timeout
            const timeoutPromise = new Promise((resolve) => {
                setTimeout(() => {
                    console.log(`[${this.getDisplayName()}] ⚠️ Timeout waiting for responses, continuing with partial results`);
                    resolve(null);
                }, 45000);
            });
            
            let puppeteerResponses = await Promise.race([
                Promise.allSettled(scrapePromises),
                timeoutPromise
            ]);
            
            // Handle timeout
            if (puppeteerResponses === null) {
                puppeteerResponses = scrapePromises.map(() => ({ 
                    status: 'rejected', 
                    reason: new Error('Timeout') 
                }));
            }
            
            // Extract results
            const results = puppeteerResponses.map((result, index) => {
                if (result && result.status === 'fulfilled') {
                    return result.value;
                } else {
                    const job = uniqueJobs[index];
                    const jobId = job.jobId || job.id;
                    return {
                        jobId: jobId,
                        error: 'Failed to get scrape response',
                        success: false
                    };
                }
            });

            const successful = results.filter(r => r && r.success).length;
            console.log(`[${this.getDisplayName()}] ✅ Received ${successful}/${results.length} successful responses`);

            // Combine results with original jobs
            const processedJobs = uniqueJobs.map((jobData, index) => {
                const job = jobData.job || jobData;
                const scrapedData = results[index];
                
                return {
                    ...jobData,
                    job: {
                        ...job,
                        data: {
                            ...job.data,
                            scraped: scrapedData || { success: false, error: 'No response' }
                        }
                    }
                };
            });

            this.processed += processedJobs.length;
            
            console.log(`[${this.getDisplayName()}] 📤 Sending ${processedJobs.length} jobs to Exporter`);
            
            // ✅ Send to exporter
            this.routeBatch(taskId, processedJobs, 'browser', 'exporter');

        } catch (error) {
            this.errors++;
            console.error(`[${this.getDisplayName()}] ❌ Error:`, error.message);
            this.sendTaskComplete(taskId, { 
                status: 'error', 
                error: error.message 
            });
        } finally {
            this.currentTask = null;
            this.isProcessing = false;
            this.currentBatchId = null;
        }
    }
}

if (require.main === module) new BrowserWorker();
module.exports = BrowserWorker;