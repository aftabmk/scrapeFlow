// child/job-submitter.js
const BaseChildProcess = require('./base');

class JobSubmitterProcess extends BaseChildProcess {
    constructor(options = {}) {
        const opts = {
            ...options,
            processType: 'job-submitter',
            queueName: options.queueName || 'job_submitter_queue',
            processingWorkers: 5  // 5 workers for parallel submission
        };
        
        super(opts);
        
        this.submitState = {
            jobsSubmitted: 0,
            maxJobs: 10,
            submitInterval: 3000,
            isSubmitting: false,
            submitTimer: null,
            events: [],
            currentEventIndex: 0,
            batchSize: 5
        };
        
        console.log(`[JobSubmitter] 🟢 Ready with ${this.processingWorkers} workers, waiting for start signal`);
        console.log(`[JobSubmitter] 📨 PID: ${process.pid}`);
    }

    // === Override _setupIPCListener ===

    _setupIPCListener() {
        console.log(`[JobSubmitter] 📨 Setting up IPC listener...`);
        
        process.on('message', async (message) => {
            console.log(`[JobSubmitter] 📨 Received message: ${message?.type || 'undefined'}`);
            
            if (!message || !message.type) {
                console.log('[JobSubmitter] ⚠️ Invalid message received');
                return;
            }

            switch (message.type) {
                case 'START_SUBMITTING':
                    console.log(`[JobSubmitter] 🚀 Received START_SUBMITTING signal`);
                    console.log(`[JobSubmitter] 📋 Events: ${message.config?.events?.length || 0}`);
                    await this._startSubmitting(message.config || {});
                    break;
                case 'SHUTDOWN':
                    console.log(`[JobSubmitter] 🛑 Received SHUTDOWN`);
                    await this.shutdown();
                    break;
                case 'GET_STATUS':
                    this._sendStatus();
                    break;
                default:
                    console.log(`[JobSubmitter] 📨 Unknown message type: ${message.type}`);
            }
        });
    }

    // === Override _startWorkers ===

    _startWorkers() {
        console.log(`[JobSubmitter] 👷 Starting ${this.processingWorkers} workers for job submission...`);
        this.workers = [];
        for (let i = 0; i < this.processingWorkers; i++) {
            this._startWorker(i);
        }
    }

    // === Override _workerLoop ===

    async _startWorker(workerId) {
        console.log(`[JobSubmitter] 👷 Worker ${workerId} started`);
        let emptyCount = 0;
        
        while (this.isRunning) {
            try {
                const job = await this.queue.dequeue();
                
                if (!job) {
                    emptyCount++;
                    if (emptyCount % 50 === 0) {
                        console.log(`[JobSubmitter] ⏳ Worker ${workerId} waiting for events...`);
                    }
                    await this._sleep(500);
                    continue;
                }
                
                emptyCount = 0;
                
                console.log(`[JobSubmitter] 🔄 Worker ${workerId} processing submission for: ${job.id}`);
                
                try {
                    const result = await this._processJob(job);
                    await this.queue.ack(job.id, result);
                    
                    console.log(`[JobSubmitter] ✅ Worker ${workerId} completed submission for ${job.id}`);
                    
                } catch (error) {
                    console.error(`[JobSubmitter] ❌ Worker ${workerId} failed submission for ${job.id}:`, error.message);
                    
                    process.send({
                        type: 'JOB_FAILED',
                        jobId: job.id,
                        error: error.message,
                        timestamp: Date.now()
                    });
                }
                
            } catch (error) {
                console.error(`[JobSubmitter] Worker ${workerId} loop error:`, error);
                await this._sleep(1000);
            }
        }
        
        console.log(`[JobSubmitter] 👷 Worker ${workerId} stopped`);
    }

    // === Override _processJob ===

    async _processJob(job) {
        const data = job.data || {};
        const eventData = data.event;
        const jobNumber = data.jobNumber || 0;
        const totalJobs = data.totalJobs || 0;
        
        if (!eventData) {
            throw new Error('No event data in job');
        }

        // Validate EXCHANGE and CONTRACT
        if (!eventData.EXCHANGE || !eventData.CONTRACT) {
            console.log(`[JobSubmitter] ❌ Event missing EXCHANGE or CONTRACT, skipping...`);
            throw new Error('Missing EXCHANGE or CONTRACT');
        }

        // Create ID: exchange-contract
        const jobId = `${eventData.EXCHANGE}-${eventData.CONTRACT}`;
        
        console.log(`[JobSubmitter] 📤 Submitting event ${jobNumber}/${totalJobs}: ${jobId}`);
        console.log(`[JobSubmitter]   EXCHANGE: ${eventData.EXCHANGE}`);
        console.log(`[JobSubmitter]   CONTRACT: ${eventData.CONTRACT}`);
        
        // Build job for orchestrator
        const submitJob = {
            id: jobId,
            type: 'analyzer',
            data: {
                id: jobId,
                event: eventData,
                exchange: eventData.EXCHANGE,
                contract: eventData.CONTRACT,
                pageUrl: eventData.PAGE_URL,
                apiUrl: eventData.API_URL,
                apiUrlBuilder: eventData.API_URL_BUILDER || null,
                referer: eventData.REFERER,
                metadata: {
                    source: 'job-submitter',
                    submittedAt: new Date().toISOString(),
                    jobNumber: jobNumber,
                    batchId: `batch_${Date.now()}`
                }
            }
        };
        
        // Send SUBMIT_JOB to orchestrator
        if (process.send) {
            process.send({
                type: 'SUBMIT_JOB',
                job: submitJob,
                jobNumber: jobNumber,
                totalJobs: totalJobs,
                eventData: eventData,
                timestamp: Date.now()
            });
            console.log(`[JobSubmitter] ✅ Submitted ${jobId} (${jobNumber}/${totalJobs})`);
            return { success: true, jobId };
        } else {
            throw new Error('process.send not available');
        }
    }

    // === Start Submitting ===

    async _startSubmitting(config = {}) {
        console.log(`[JobSubmitter] 📤 _startSubmitting called`);
        
        if (this.submitState.isSubmitting) {
            console.log(`[JobSubmitter] ⚠️ Already submitting, ignoring duplicate start`);
            return;
        }
        
        this.submitState.isSubmitting = true;
        
        this.submitState.events = config.events || [];
        this.submitState.maxJobs = config.maxJobs || this.submitState.events.length || 10;
        this.submitState.submitInterval = config.submitInterval || 3000;
        this.submitState.jobsSubmitted = 0;
        this.submitState.currentEventIndex = 0;
        
        console.log(`[JobSubmitter] 📤 Starting to submit ${this.submitState.maxJobs} events with ${this.processingWorkers} workers...`);
        console.log(`[JobSubmitter] ⏱️ Interval: ${this.submitState.submitInterval}ms`);
        console.log(`[JobSubmitter] 📋 Events count: ${this.submitState.events.length}`);
        
        // Send started event
        if (process.send) {
            process.send({
                type: 'SUBMITTER_STARTED',
                maxJobs: this.submitState.maxJobs,
                submitInterval: this.submitState.submitInterval,
                timestamp: Date.now()
            });
        }

        // Submit first batch of events
        await this._submitNextBatch();
        
        // Start interval for subsequent batches
        this.submitState.submitTimer = setInterval(async () => {
            if (this.submitState.jobsSubmitted >= this.submitState.maxJobs || !this.isRunning) {
                clearInterval(this.submitState.submitTimer);
                this.submitState.isSubmitting = false;
                console.log(`[JobSubmitter] ✅ All ${this.submitState.maxJobs} events submitted!`);
                if (process.send) {
                    process.send({
                        type: 'SUBMITTER_COMPLETE',
                        totalJobs: this.submitState.jobsSubmitted,
                        timestamp: Date.now()
                    });
                }
                return;
            }
            await this._submitNextBatch();
        }, this.submitState.submitInterval);
    }

    // === Submit Next Batch ===

    async _submitNextBatch() {
        // Submit up to batchSize events at a time (one per worker)
        const batchSize = Math.min(this.submitState.batchSize, this.submitState.maxJobs - this.submitState.jobsSubmitted);
        const batch = [];
        
        for (let i = 0; i < batchSize; i++) {
            if (this.submitState.currentEventIndex >= this.submitState.events.length) break;
            
            const eventData = this.submitState.events[this.submitState.currentEventIndex];
            const jobNumber = this.submitState.jobsSubmitted + 1;
            
            // Validate event
            if (!eventData.EXCHANGE || !eventData.CONTRACT) {
                console.log(`[JobSubmitter] ❌ Event ${jobNumber} missing EXCHANGE or CONTRACT, skipping...`);
                this.submitState.jobsSubmitted++;
                this.submitState.currentEventIndex++;
                continue;
            }
            
            // Create job for worker
            const jobId = `${eventData.EXCHANGE}-${eventData.CONTRACT}`;
            const job = {
                id: jobId,
                data: {
                    event: eventData,
                    jobNumber: jobNumber,
                    totalJobs: this.submitState.maxJobs
                }
            };
            
            // Enqueue to durable queue (workers will pick up)
            await this.queue.enqueue(job);
            
            batch.push({ eventData, jobNumber, jobId });
            this.submitState.jobsSubmitted++;
            this.submitState.currentEventIndex++;
        }
        
        if (batch.length > 0) {
            console.log(`[JobSubmitter] 📤 Queued ${batch.length} events for workers (${this.submitState.jobsSubmitted}/${this.submitState.maxJobs})`);
        }
    }

    // === Send Status ===

    _sendStatus() {
        if (process.send) {
            process.send({
                type: 'STATUS',
                processType: this.processType,
                jobsSubmitted: this.submitState.jobsSubmitted,
                maxJobs: this.submitState.maxJobs,
                isSubmitting: this.submitState.isSubmitting,
                eventsRemaining: this.submitState.events.length - this.submitState.currentEventIndex,
                activeJobs: this.queue.getInFlightCount(),
                queueSize: this.queue.queue.getSize(),
                pid: process.pid
            });
        }
    }

    // === Shutdown ===

    async shutdown() {
        console.log(`[JobSubmitter] 🛑 Shutting down...`);
        this.isRunning = false;
        this.submitState.isSubmitting = false;
        
        if (this.submitState.submitTimer) {
            clearInterval(this.submitState.submitTimer);
            this.submitState.submitTimer = null;
        }
        
        if (process.send) {
            process.send({ type: 'SHUTDOWN_COMPLETE' });
        }
        setTimeout(() => process.exit(0), 500);
    }
}

if (require.main === module) {
    console.log('[JobSubmitter] 🚀 Starting job-submitter process with 5 workers...');
    new JobSubmitterProcess();
}

module.exports = JobSubmitterProcess;