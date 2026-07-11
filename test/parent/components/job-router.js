// parent/components/job-router.js
const { EventEmitter } = require('events');

class JobRouter extends EventEmitter {
    constructor(options = {}) {
        super();
        this.processManager = options.processManager;
        this.submitJobFn = null;
        this.pendingSubmissions = [];
        this.isProcessing = false;
        this.pipeline = options.pipeline || ['analyzer', 'browser', 'exporter'];
        this.stageMap = {
            'analyzer': 'analyzer',
            'browser': 'browser',
            'exporter': 'exporter'
        };
    }

    setSubmitJobFn(fn) {
        this.submitJobFn = fn;
        return this;
    }

    /**
     * Route to next stage in pipeline
     */
    async routeToNext(message) {
        const result = message.result;
        const currentType = message.processType || message.type || 'unknown';
        
        console.log(`[JobRouter] 🔄 Routing ${message.jobId} from ${currentType}`);

        // ✅ Special case: job-submitter → analyzer (first stage)
        if (currentType === 'job-submitter' || currentType === 'submitter') {
            console.log(`[JobRouter] 📤 Routing from job-submitter to analyzer (first stage)`);
            await this._routeToAnalyzer(message, result);
            return;
        }

        // Find current stage index
        const currentIndex = this.pipeline.indexOf(currentType);
        
        if (currentIndex === -1) {
            console.error(`[JobRouter] ❌ Unknown stage: ${currentType}`);
            this.emit('error', { jobId: message.jobId, from: currentType, error: `Unknown stage: ${currentType}` });
            return;
        }

        const nextIndex = currentIndex + 1;
        
        if (nextIndex >= this.pipeline.length) {
            // Job completed all stages
            console.log(`[JobRouter] 🎉 Job ${message.jobId} fully completed!`);
            this.emit('jobComplete', { jobId: message.jobId, result });
            return;
        }

        const nextType = this.pipeline[nextIndex];
        console.log(`[JobRouter] 🔄 Routing ${message.jobId} from ${currentType} to ${nextType}`);

        const nextJob = this._buildJobForStage(nextType, message, result);
        
        try {
            await this.submitJobFn(nextJob);
            console.log(`[JobRouter] ✅ ${nextType} job created for ${message.jobId}`);
            this.emit('routed', { jobId: message.jobId, from: currentType, to: nextType });
        } catch (error) {
            console.error(`[JobRouter] ❌ Failed to route to ${nextType}:`, error.message);
            this.emit('error', { jobId: message.jobId, from: currentType, to: nextType, error });
        }
    }

    /**
     * Route from job-submitter to analyzer
     */
    async _routeToAnalyzer(message, result) {
        const jobId = message.jobId;
        console.log(`[JobRouter] 📤 Creating analyzer job for ${jobId}`);

        const analyzerJob = {
            id: jobId,
            type: 'analyzer',
            data: {
                id: jobId,
                event: result.event || {},
                exchange: result.exchange,
                contract: result.contract,
                pageUrl: result.pageUrl,
                apiUrl: result.apiUrl,
                apiUrlBuilder: result.apiUrlBuilder,
                referer: result.referer,
                metadata: result.metadata || {},
                submittedAt: result.submittedAt || new Date().toISOString()
            }
        };

        try {
            await this.submitJobFn(analyzerJob);
            console.log(`[JobRouter] ✅ Analyzer job created for ${jobId}`);
            this.emit('routed', { jobId, from: 'job-submitter', to: 'analyzer' });
        } catch (error) {
            console.error(`[JobRouter] ❌ Failed to create analyzer job:`, error.message);
            this.emit('error', { jobId, from: 'job-submitter', to: 'analyzer', error });
        }
    }

    _buildJobForStage(stage, message, result) {
        const jobId = message.jobId;
        
        switch (stage) {
            case 'analyzer':
                return {
                    id: jobId,
                    type: 'analyzer',
                    data: {
                        id: jobId,
                        event: result.event || {},
                        exchange: result.exchange,
                        contract: result.contract,
                        pageUrl: result.pageUrl,
                        apiUrl: result.apiUrl,
                        apiUrlBuilder: result.apiUrlBuilder,
                        referer: result.referer,
                        analysisJobId: message.jobId,
                        metadata: result.metadata || {}
                    }
                };
            case 'browser':
                return {
                    id: jobId,
                    type: 'browser',
                    data: {
                        id: jobId,
                        event: result.event || {},
                        exchange: result.exchange,
                        contract: result.contract,
                        pageUrl: result.pageUrl,
                        apiUrl: result.apiUrl,
                        apiUrlBuilder: result.apiUrlBuilder,
                        referer: result.referer,
                        analysisJobId: message.jobId,
                        analyzedAt: result.analyzedAt,
                        metadata: result.metadata || {}
                    }
                };
            case 'exporter':
                return {
                    id: jobId,
                    type: 'exporter',
                    data: {
                        id: jobId,
                        event: result.event || {},
                        exchange: result.exchange,
                        contract: result.contract,
                        pageUrl: result.pageUrl,
                        apiUrl: result.apiUrl,
                        referer: result.referer,
                        browserJobId: message.jobId,
                        scrapedAt: result.scrapedAt || new Date().toISOString(),
                        metadata: result.metadata || {}
                    }
                };
            default:
                throw new Error(`Unknown stage: ${stage}`);
        }
    }

    /**
     * Handle job submission from job-submitter
     */
    async handleJobSubmission(message) {
        const { job, jobNumber, totalJobs, eventData } = message;
        
        if (this.isProcessing) {
            this.pendingSubmissions.push(message);
            return;
        }
        
        this.isProcessing = true;
        
        try {
            const analyzerProcess = await this.processManager.waitForProcess('analyzer');
            if (!analyzerProcess) {
                console.error(`[JobRouter] ❌ Analyzer not available for job ${jobNumber}`);
                this.emit('error', { jobNumber, error: 'Analyzer not available' });
                return;
            }
            
            console.log(`[JobRouter] 📤 Submitting job ${jobNumber}/${totalJobs}: ${job.id}`);
            if (eventData) {
                console.log(`[JobRouter] 📋 ${eventData.EXCHANGE} - ${eventData.CONTRACT}`);
            }
            
            const result = await this.submitJobFn(job);
            console.log(`[JobRouter] ✅ Job ${result.jobId} submitted (${jobNumber}/${totalJobs})`);
            this.emit('submitted', { jobNumber, totalJobs, jobId: result.jobId, eventData });
            
        } catch (error) {
            console.error(`[JobRouter] ❌ Job ${jobNumber} failed:`, error.message);
            this.emit('error', { jobNumber, error: error.message });
        } finally {
            this.isProcessing = false;
            this._processNext();
        }
    }

    _processNext() {
        if (this.pendingSubmissions.length > 0 && !this.isProcessing) {
            const next = this.pendingSubmissions.shift();
            this.handleJobSubmission(next);
        }
    }

    /**
     * Get pipeline status
     */
    getPipeline() {
        return {
            stages: this.pipeline,
            currentPosition: this.pipeline.length,
            pendingSubmissions: this.pendingSubmissions.length,
            isProcessing: this.isProcessing
        };
    }

    /**
     * Set pipeline stages
     */
    setPipeline(stages) {
        this.pipeline = stages;
        return this;
    }
}

module.exports = JobRouter;