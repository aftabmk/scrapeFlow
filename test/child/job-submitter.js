// child/job-submitter.js
const BaseChildProcess = require('./base');

class JobSubmitterProcess extends BaseChildProcess {
    constructor(options = {}) {
        const opts = {
            ...options,
            processType: 'job-submitter',
            queueName: options.queueName || 'job_submitter_queue'
        };
        
        super(opts);
        
        this.submitState = {
            jobsSubmitted: 0,
            maxJobs: 10,
            submitInterval: 300,
            isSubmitting: false,
            submitTimer: null,
            events: [],
            currentEventIndex: 0
        };
        
        console.log(`[JobSubmitter] 🟢 Ready with 5 workers, waiting for start signal`);
        console.log(`[JobSubmitter] 📨 PID: ${process.pid}`);
    }

    // ✅ Override _setupIPCListener to handle START_SUBMITTING
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

    // ✅ Override _processJob to handle submission logic
    async _processJob(job) {
        // Each worker will process one event submission
        // This is where the actual job submission happens
        console.log(`[JobSubmitter] 🔄 Worker processing submission for job: ${job.id}`);
        
        // The job data contains the event to submit
        const eventData = job.data?.event;
        const jobNumber = job.data?.jobNumber || 0;
        const totalJobs = job.data?.totalJobs || 0;
        
        if (!eventData) {
            console.log('[JobSubmitter] ⚠️ No event data in job');
            return { success: false, error: 'No event data' };
        }

        // Validate EXCHANGE and CONTRACT
        if (!eventData.EXCHANGE || !eventData.CONTRACT) {
            console.log(`[JobSubmitter] ❌ Event missing EXCHANGE or CONTRACT, skipping...`);
            return { success: false, error: 'Missing EXCHANGE or CONTRACT' };
        }

        // Create ID: exchange-contract
        const jobId = `${eventData.EXCHANGE}-${eventData.CONTRACT}`;
        
        console.log(`[JobSubmitter] 📤 Submitting event ${jobNumber}/${totalJobs}: ${jobId}`);
        console.log(`[JobSubmitter]   EXCHANGE: ${eventData.EXCHANGE}`);
        console.log(`[JobSubmitter]   CONTRACT: ${eventData.CONTRACT}`);
        
        const submitJob = {
            id: jobId,
            type: 'analyzer',
            data: {
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
            console.log('[JobSubmitter] ❌ process.send not available');
            return { success: false, error: 'process.send not available' };
        }
    }

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
        
        console.log(`[JobSubmitter] 📤 Starting to submit ${this.submitState.maxJobs} events with 5 workers...`);
        console.log(`[JobSubmitter] ⏱️ Interval: ${this.submitState.submitInterval}ms`);
        console.log(`[JobSubmitter] 📋 Events count: ${this.submitState.events.length}`);
        
        // ✅ Send started event
        if (process.send) {
            process.send({
                type: 'SUBMITTER_STARTED',
                maxJobs: this.submitState.maxJobs,
                submitInterval: this.submitState.submitInterval,
                timestamp: Date.now()
            });
        }

        // ✅ Submit first batch of events to workers
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

    async _submitNextBatch() {
        // ✅ Submit up to 5 events at a time (one per worker)
        const batchSize = Math.min(5, this.submitState.maxJobs - this.submitState.jobsSubmitted);
        const batch = [];
        
        for (let i = 0; i < batchSize; i++) {
            if (this.submitState.currentEventIndex >= this.submitState.events.length) break;
            const eventData = this.submitState.events[this.submitState.currentEventIndex];
            const jobNumber = this.submitState.jobsSubmitted + 1;
            
            // ✅ Enqueue each event as a job for workers to process
            const job = {
                id: `${eventData.EXCHANGE}-${eventData.CONTRACT}`,
                data: {
                    event: eventData,
                    jobNumber: jobNumber,
                    totalJobs: this.submitState.maxJobs
                }
            };
            
            // ✅ Enqueue to durable queue (workers will pick up)
            await this.queue.enqueue(job);
            
            batch.push({ eventData, jobNumber });
            this.submitState.jobsSubmitted++;
            this.submitState.currentEventIndex++;
        }
        
        console.log(`[JobSubmitter] 📤 Queued ${batch.length} events for workers (${this.submitState.jobsSubmitted}/${this.submitState.maxJobs})`);
    }

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