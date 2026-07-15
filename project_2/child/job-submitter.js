// child/job-submitter.js
const BaseChildProcess = require('./base');
const Message = require('../messaging/message');
const { MessageTypes, MessageDestinations } = require('../messaging/message-types');

class JobSubmitterProcess extends BaseChildProcess {
    constructor(options = {}) {
        const opts = {
            ...options,
            processType: 'job-submitter',
            queueName: options.queueName || 'job_submitter_queue',
            processingWorkers: 5
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

    _setupMessageHandlers() {
        super._setupMessageHandlers();

        this.messageHandlers.set(MessageTypes.START_SUBMITTING, (msg) => {
            console.log(`[JobSubmitter] 🚀 Received START_SUBMITTING signal`);
            console.log(`[JobSubmitter] 📋 Events: ${msg.payload.config?.events?.length || 0}`);
            this._startSubmitting(msg.payload.config || {});
        });

        this.messageHandlers.set('JOB_SUBMITTED', (msg) => {
            console.log(`[JobSubmitter] ✅ Job ${msg.payload.jobId} submitted (${msg.payload.jobNumber}/${msg.payload.totalJobs})`);
        });
    }

    async _processJob(job) {
        const data = job.data || {};
        const eventData = data.event;
        const jobNumber = data.jobNumber || 0;
        const totalJobs = data.totalJobs || 0;

        if (!eventData) {
            throw new Error('No event data in job');
        }

        if (!eventData.EXCHANGE || !eventData.CONTRACT) {
            console.log(`[JobSubmitter] ❌ Event missing EXCHANGE or CONTRACT, skipping...`);
            throw new Error('Missing EXCHANGE or CONTRACT');
        }

        const jobId = `${eventData.EXCHANGE}-${eventData.CONTRACT}`;

        console.log(`[JobSubmitter] 📤 Submitting event ${jobNumber}/${totalJobs}: ${jobId}`);
        console.log(`[JobSubmitter]   EXCHANGE: ${eventData.EXCHANGE}`);
        console.log(`[JobSubmitter]   CONTRACT: ${eventData.CONTRACT}`);

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

        const msg = new Message({
            from: this.processType,
            to: MessageDestinations.ORCHESTRATOR,
            type: MessageTypes.SUBMIT_JOB,
            payload: {
                job: submitJob,
                jobNumber: jobNumber,
                totalJobs: totalJobs,
                eventData: eventData,
                timestamp: Date.now()
            }
        });
        msg.send();

        console.log(`[JobSubmitter] ✅ Submitted ${jobId} (${jobNumber}/${totalJobs})`);
        return { success: true, jobId };
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

        console.log(`[JobSubmitter] 📤 Starting to submit ${this.submitState.maxJobs} events with ${this.processingWorkers} workers...`);
        console.log(`[JobSubmitter] ⏱️ Interval: ${this.submitState.submitInterval}ms`);
        console.log(`[JobSubmitter] 📋 Events count: ${this.submitState.events.length}`);

        const startedMsg = new Message({
            from: this.processType,
            to: MessageDestinations.ORCHESTRATOR,
            type: MessageTypes.SUBMITTER_STARTED,
            payload: {
                maxJobs: this.submitState.maxJobs,
                submitInterval: this.submitState.submitInterval,
                timestamp: Date.now()
            }
        });
        startedMsg.send();

        await this._submitNextBatch();

        this.submitState.submitTimer = setInterval(async () => {
            if (this.submitState.jobsSubmitted >= this.submitState.maxJobs || !this.isRunning) {
                clearInterval(this.submitState.submitTimer);
                this.submitState.isSubmitting = false;
                console.log(`[JobSubmitter] ✅ All ${this.submitState.maxJobs} events submitted!`);

                const completeMsg = new Message({
                    from: this.processType,
                    to: MessageDestinations.ORCHESTRATOR,
                    type: MessageTypes.SUBMITTER_COMPLETE,
                    payload: {
                        totalJobs: this.submitState.jobsSubmitted,
                        timestamp: Date.now()
                    }
                });
                completeMsg.send();
                return;
            }
            await this._submitNextBatch();
        }, this.submitState.submitInterval);
    }

    async _submitNextBatch() {
        const batchSize = Math.min(this.submitState.batchSize, this.submitState.maxJobs - this.submitState.jobsSubmitted);

        for (let i = 0; i < batchSize; i++) {
            if (this.submitState.currentEventIndex >= this.submitState.events.length) break;

            const eventData = this.submitState.events[this.submitState.currentEventIndex];
            const jobNumber = this.submitState.jobsSubmitted + 1;

            if (!eventData.EXCHANGE || !eventData.CONTRACT) {
                console.log(`[JobSubmitter] ❌ Event ${jobNumber} missing EXCHANGE or CONTRACT, skipping...`);
                this.submitState.jobsSubmitted++;
                this.submitState.currentEventIndex++;
                continue;
            }

            const jobId = `${eventData.EXCHANGE}-${eventData.CONTRACT}`;
            const job = {
                id: jobId,
                data: {
                    event: eventData,
                    jobNumber: jobNumber,
                    totalJobs: this.submitState.maxJobs
                }
            };

            await this.queue.enqueue(job);
            this.submitState.jobsSubmitted++;
            this.submitState.currentEventIndex++;
        }

        console.log(`[JobSubmitter] 📤 Queued events (${this.submitState.jobsSubmitted}/${this.submitState.maxJobs})`);
    }

    async shutdown() {
        console.log(`[JobSubmitter] 🛑 Shutting down...`);
        this.isRunning = false;
        this.submitState.isSubmitting = false;

        if (this.submitState.submitTimer) {
            clearInterval(this.submitState.submitTimer);
            this.submitState.submitTimer = null;
        }

        const shutdownMsg = new Message({
            from: this.processType,
            to: MessageDestinations.ORCHESTRATOR,
            type: MessageTypes.SHUTDOWN_COMPLETE,
            payload: { pid: process.pid }
        });
        shutdownMsg.send();

        setTimeout(() => process.exit(0), 500);
    }
}

if (require.main === module) {
    console.log('[JobSubmitter] 🚀 Starting job-submitter process with 5 workers...');
    new JobSubmitterProcess();
}

module.exports = JobSubmitterProcess;