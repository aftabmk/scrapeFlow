// messaging/pipeline.js
const { EventEmitter } = require('events');
const Message = require('./message');
const { MessageTypes, MessageDestinations } = require('./message-types');

class Pipeline extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.stages = options.stages || ['job-submitter', 'analyzer', 'browser', 'exporter'];
        this.orchestrator = options.orchestrator;
        this.processManager = options.processManager;
        this.submitJobFn = options.submitJobFn || null;
        this.stageMap = {};
        this.stageHandlers = new Map();
        this.completedJobs = new Set();
        this.isSubmitting = false;
        this.submitTimer = null;
        this.submitState = {
            jobsSubmitted: 0,
            maxJobs: 10,
            submitInterval: 3000,
            events: [],
            currentEventIndex: 0
        };
        this.pendingSubmissions = [];
        this.isProcessingSubmission = false;

        // Build stage map
        this.stages.forEach((stage, index) => {
            this.stageMap[stage] = index;
        });

        console.log(`[Pipeline] ✅ Initialized with stages: ${this.stages.join(' → ')}`);
        console.log(`[Pipeline] 📋 Process mapping:`, this.stageProcessMap);
    }

    // === Configuration ===

    setSubmitJobFn(fn) {
        this.submitJobFn = fn;
        return this;
    }

    setOrchestrator(orchestrator) {
        this.orchestrator = orchestrator;
        return this;
    }

    setProcessManager(processManager) {
        this.processManager = processManager;
        return this;
    }

    registerStageHandler(stage, handler) {
        this.stageHandlers.set(stage, handler);
        return this;
    }

    // === Start Pipeline ===

    async start(config = {}) {
        console.log('[Pipeline] 🚀 Starting pipeline...');
        console.log(`[Pipeline] 📋 Events: ${config.events?.length || 0}`);
        console.log(`[Pipeline] 📋 Stages: ${this.stages.join(' → ')}`);

        await this._startJobSubmitter(config);

        console.log('[Pipeline] ✅ Pipeline started');
        this.emit('started', config);
    }

    // === Submitter Management ===

    async _startJobSubmitter(config = {}) {
        console.log('[Pipeline] 📤 Starting job submitter...');

        if (this.isSubmitting) {
            console.log('[Pipeline] ⚠️ Submitter already running');
            return;
        }

        // ✅ Find job-submitter process (using mapped name)
        const submitterProcess = await this._waitForProcess('job-submitter');
        if (!submitterProcess) {
            console.error('[Pipeline] ❌ Job-submitter process not found!');
            this.emit('error', { stage: 'job-submitter', error: 'Job-submitter not found' });
            return;
        }

        this.isSubmitting = true;
        this.submitState.events = config.events || [];
        this.submitState.maxJobs = config.maxJobs || this.submitState.events.length || 10;
        this.submitState.submitInterval = config.submitInterval || 3000;
        this.submitState.jobsSubmitted = 0;
        this.submitState.currentEventIndex = 0;

        console.log(`[Pipeline] 📤 Job Submitter started: ${this.submitState.maxJobs} events`);
        console.log(`[Pipeline] 📤 Found job-submitter process: ${submitterProcess.pid}`);

        // Send START_SUBMITTING to job-submitter
        const msg = new Message({
            from: MessageDestinations.ORCHESTRATOR,
            to: MessageDestinations.JOB_SUBMITTER,
            type: MessageTypes.START_SUBMITTING,
            payload: {
                parentPid: process.pid,
                config: {
                    maxJobs: this.submitState.maxJobs,
                    submitInterval: this.submitState.submitInterval,
                    events: this.submitState.events
                }
            }
        });

        try {
            msg.send(submitterProcess.child);
            console.log('[Pipeline] ✅ START_SUBMITTING sent to job-submitter');
            this.emit('submitterStarted', this.submitState);
        } catch (error) {
            console.error('[Pipeline] ❌ Failed to send START_SUBMITTING:', error.message);
            this.emit('error', { stage: 'job-submitter', error });
        }
    }

    async _waitForProcess(stage, timeout = 30000) {
        if (!this.processManager) {
            console.error('[Pipeline] ❌ ProcessManager not available');
            return null;
        }

        // ✅ Map stage name to actual process type
        const processType = stage;
        console.log(`[Pipeline] 🔍 Looking for process: ${processType} (from stage: ${stage})`);

        const startTime = Date.now();
        let attemptCount = 0;

        while (Date.now() - startTime < timeout) {
            attemptCount++;
            const process = this.processManager.getProcess(processType);
            
            if (process) {
                console.log(`[Pipeline] ✅ Found process: ${processType} (${process.pid}) after ${attemptCount} attempts`);
                return process;
            }

            // Log every 10 attempts (5 seconds)
            if (attemptCount % 10 === 0) {
                const elapsed = Math.floor((Date.now() - startTime) / 1000);
                console.log(`[Pipeline] ⏳ Still waiting for ${processType}... (${elapsed}s elapsed)`);
                
                // Log current processes for debugging
                const allProcesses = this.processManager.getAllProcesses();
                console.log(`[Pipeline] 📋 Current processes:`, allProcesses.map(p => `${p.type} (${p.pid})`).join(', '));
            }

            await this._sleep(500);
        }

        console.error(`[Pipeline] ❌ Process not found: ${processType} (stage: ${stage})`);
        return null;
    }

    // === Handle SUBMIT_JOB ===

    async handleSubmitJob(message) {
        const { job, jobNumber, totalJobs, eventData } = message.payload;

        console.log(`[Pipeline] 📨 Received SUBMIT_JOB: ${job.id} (${jobNumber}/${totalJobs})`);

        if (this.isProcessingSubmission) {
            this.pendingSubmissions.push(message);
            return;
        }

        this.isProcessingSubmission = true;

        try {
            const analyzerProcess = await this._waitForProcess('analyzer');
            if (!analyzerProcess) {
                console.error('[Pipeline] ❌ Analyzer not available');
                this.emit('error', { stage: 'analyzer', error: 'Analyzer not available' });
                return;
            }

            console.log(`[Pipeline] 📤 Routing ${job.id} to analyzer (${jobNumber}/${totalJobs})`);

            const analyzerJob = this._buildJobForStage('analyzer', job, null);
            await this.submitJobFn(analyzerJob);

            console.log(`[Pipeline] ✅ ${job.id} submitted to analyzer (${jobNumber}/${totalJobs})`);

            this._sendSubmissionResponse(message, job.id, jobNumber, totalJobs, eventData);
            this.emit('jobSubmitted', { jobId: job.id, jobNumber, totalJobs, eventData });

        } catch (error) {
            console.error(`[Pipeline] ❌ Job submission failed:`, error.message);
            this.emit('error', { stage: 'job-submitter', error });
        } finally {
            this.isProcessingSubmission = false;
            this._processNextPending();
        }
    }

    _sendSubmissionResponse(message, jobId, jobNumber, totalJobs, eventData) {
        const response = new Message({
            from: MessageDestinations.ORCHESTRATOR,
            to: MessageDestinations.JOB_SUBMITTER,
            type: 'JOB_SUBMITTED',
            payload: {
                jobNumber,
                totalJobs,
                jobId,
                eventData
            },
            correlationId: message.requestId
        });
        response.send();
    }

    _processNextPending() {
        if (this.pendingSubmissions.length > 0 && !this.isProcessingSubmission) {
            const next = this.pendingSubmissions.shift();
            this.handleSubmitJob(next);
        }
    }

    // === Job Advancement ===

    async advance(message) {
        const currentStage = message.from;
        const jobId = message.payload.jobId;
        const result = message.payload.result;

        console.log(`[Pipeline] 🔄 Advancing job ${jobId} from ${currentStage}`);

        if (currentStage === 'job-submitter') {
            await this.handleSubmitJob(message);
            return;
        }

        if (!this.stageMap.hasOwnProperty(currentStage)) {
            console.error(`[Pipeline] ❌ Unknown stage: ${currentStage}`);
            this.emit('error', { jobId, currentStage, error: `Unknown stage: ${currentStage}` });
            return;
        }

        const currentIndex = this.stageMap[currentStage];
        const nextIndex = currentIndex + 1;

        if (nextIndex >= this.stages.length) {
            console.log(`[Pipeline] 🎉 Job ${jobId} fully completed!`);
            this.completedJobs.add(jobId);
            this.emit('complete', { jobId, result });
            return;
        }

        const nextStage = this.stages[nextIndex];
        console.log(`[Pipeline] 🔄 Routing ${jobId} from ${currentStage} → ${nextStage}`);

        const nextJob = this._buildJobForStage(nextStage, null, result, jobId);

        if (this.stageHandlers.has(nextStage)) {
            const handler = this.stageHandlers.get(nextStage);
            try {
                await handler(nextJob, message);
            } catch (error) {
                console.error(`[Pipeline] ❌ Stage handler failed for ${nextStage}:`, error.message);
                this.emit('error', { jobId, stage: nextStage, error });
                return;
            }
        }

        try {
            await this.submitJobFn(nextJob);
            console.log(`[Pipeline] ✅ ${nextStage} job created for ${jobId}`);
            this.emit('routed', { jobId, from: currentStage, to: nextStage });
        } catch (error) {
            console.error(`[Pipeline] ❌ Failed to route to ${nextStage}:`, error.message);
            this.emit('error', { jobId, from: currentStage, to: nextStage, error });
        }
    }

    // === Job Building ===

    _buildJobForStage(stage, originalJob, result, jobId = null) {
        const id = originalJob?.id || jobId || `job_${Date.now()}`;
        const event = originalJob?.data?.event || result?.event || {};
        const metadata = originalJob?.data?.metadata || result?.metadata || {};

        const payload = {
            id: id,
            type: stage,
            data: {
                id: id,
                event: event,
                exchange: event.EXCHANGE,
                contract: event.CONTRACT,
                pageUrl: event.PAGE_URL,
                apiUrl: event.API_URL,
                apiUrlBuilder: event.API_URL_BUILDER,
                referer: event.REFERER,
                metadata: metadata
            }
        };

        switch (stage) {
            case 'analyzer':
                payload.data.analyzedAt = result?.analyzedAt || new Date().toISOString();
                payload.data.submittedAt = result?.submittedAt || new Date().toISOString();
                break;

            case 'browser':
                payload.data.analysisJobId = id;
                payload.data.analyzedAt = result?.analyzedAt || new Date().toISOString();
                break;

            case 'exporter':
                payload.data.browserJobId = id;
                payload.data.scrapedAt = result?.scrapedAt || new Date().toISOString();
                break;

            default:
                throw new Error(`Unknown stage: ${stage}`);
        }

        return payload;
    }

    // === Status ===

    isComplete(jobId) {
        return this.completedJobs.has(jobId);
    }

    getNextStage(currentStage) {
        if (!this.stageMap.hasOwnProperty(currentStage)) {
            return null;
        }
        const currentIndex = this.stageMap[currentStage];
        const nextIndex = currentIndex + 1;
        return nextIndex < this.stages.length ? this.stages[nextIndex] : null;
    }

    getStatus() {
        return {
            stages: this.stages,
            currentStage: this.stages.length > 1 ? this.stages[1] : null,
            totalStages: this.stages.length,
            completedJobs: this.completedJobs.size,
            isSubmitting: this.isSubmitting,
            jobsSubmitted: this.submitState.jobsSubmitted,
            maxJobs: this.submitState.maxJobs,
            pendingSubmissions: this.pendingSubmissions.length
        };
    }

    getSubmitterStatus() {
        return {
            isSubmitting: this.isSubmitting,
            jobsSubmitted: this.submitState.jobsSubmitted,
            maxJobs: this.submitState.maxJobs,
            eventsRemaining: this.submitState.events.length - this.submitState.currentEventIndex
        };
    }

    reset() {
        this.completedJobs.clear();
        this.pendingSubmissions = [];
        this.isSubmitting = false;
        this.isProcessingSubmission = false;
        this.submitState.jobsSubmitted = 0;
        this.submitState.currentEventIndex = 0;
        this.emit('reset');
        return this;
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = Pipeline;