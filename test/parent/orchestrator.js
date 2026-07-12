// parent/orchestrator.js
const { EventEmitter } = require('events');
const ProcessCreator = require('./components/process-creator');
const SQLiteServerManager = require('./components/sqlite-server-manager');

class Orchestrator extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.isRunning = true;
        this.allProcessesReady = false;
        this.serverReady = false;
        this.readyChildren = 0;
        this.aliveChildren = 0;
        this.expectedChildren = 0;
        this.pendingSubmissions = [];
        this.isProcessingSubmission = false;
        this.heartbeatTimeout = options.heartbeatTimeout || 15000;
        this.options = options;

        // ✅ SQLite Server Manager
        this.sqliteManager = new SQLiteServerManager({
            dbPath: options.dbPath || './data/queue.db',
            readWorkers: options.readWorkers || 3,
            writeWorkers: options.writeWorkers || 1,
            startTimeout: options.sqliteTimeout || 10000,
            restartDelay: options.restartDelay || 2000,
            queueNames: options.queueNames || ['analyzer', 'browser', 'exporter', 'job-submitter']
        });

        // ✅ Process Creator
        this.processCreator = new ProcessCreator({
            restartDelay: options.restartDelay || 2000
        });

        // ✅ Setup listeners
        this._setupSQLiteListeners();
        this._setupProcessCreatorListeners();
        this._startHeartbeatMonitor();
    }

    // === SQLite Server Listeners ===

    _setupSQLiteListeners() {
        this.sqliteManager.on('ready', (info) => {
            console.log('[Orchestrator] ✅ SQLite Server ready');
        });

        this.sqliteManager.on('allTablesCreated', () => {
            console.log('[Orchestrator] 🎯 All SQLite tables created');
            this.serverReady = true;
            this._checkAllReady();
        });

        this.sqliteManager.on('shutdown', () => {
            console.log('[Orchestrator] SQLite Server shutdown');
        });
    }

    // === ProcessCreator Listeners ===

    _setupProcessCreatorListeners() {
        this.processCreator.on('processReady', (data) => {
            // Child sent ALIVE (normal startup, no recover)
            console.log(`[Orchestrator] ✅ Process ${data.pid} (${data.type}) alive`);
            this.aliveChildren++;
            this.emit('processReady', data);
            this._checkAllReady();
        });

        this.processCreator.on('processReadyAfterRecover', (data) => {
            // Child sent READY (after recover)
            console.log(`[Orchestrator] ✅ Process ${data.pid} (${data.type}) ready after recover`);
            this.readyChildren++;
            this.emit('processReadyAfterRecover', data);
            this._checkAllReady();
        });

        this.processCreator.on('processTimeout', (data) => {
            this.emit('processTimeout', data);
        });

        this.processCreator.on('exit', (processInfo, code, signal) => {
            console.log(`[Orchestrator] ⚠️ Process ${processInfo.pid} (${processInfo.type}) exited with code ${code}`);
            this.aliveChildren--;
            this.readyChildren--;
            this.allProcessesReady = false;
            this.emit('processExit', { pid: processInfo.pid, type: processInfo.type, code, signal });
        });

        this.processCreator.on('message', (processInfo, message) => {
            this._handleChildMessage(processInfo, message);
        });
    }

    // === Start SQLite Server ===

    async startSQLiteServer(options = {}) {
        return this.sqliteManager.start();
    }

    // === Process Creation ===

    async createProcess(options = {}) {
        const { type } = options;
        this.expectedChildren++;
        
        // ✅ Pass serverReady flag to child
        const processInfo = await this.processCreator.createProcess({
            ...options,
            serverReady: this.serverReady
        });

        return processInfo;
    }

    async createAllProcesses(processConfigs) {
        const promises = processConfigs.map(config => this.createProcess(config));
        return Promise.all(promises);
    }

    // === Check All Ready ===

    _checkAllReady() {
        // ✅ All children are alive (sent ALIVE)
        const allAlive = this.aliveChildren === this.expectedChildren && this.expectedChildren > 0;
        
        // ✅ Server is ready (all tables created)
        const serverReady = this.serverReady;
        
        // ✅ All children are ready (sent READY after recover)
        const allReady = this.readyChildren === this.expectedChildren && this.expectedChildren > 0;

        if (allAlive && serverReady && allReady && !this.allProcessesReady) {
            this.allProcessesReady = true;
            console.log('[Orchestrator] 🎯 All processes are ready!');
            this.emit('allProcessesReady');
        }
    }

    // === Message Handling ===

    _handleChildMessage(processInfo, message) {
        if (!message || !message.type) return;

        switch (message.type) {
            case 'HEARTBEAT':
                processInfo.lastHeartbeat = Date.now();
                this.emit('heartbeat', { pid: processInfo.pid, stats: message.stats });
                break;

            case 'JOB_QUEUED':
                console.log(`[Orchestrator] 📝 Job ${message.jobId} queued in ${processInfo.type}`);
                this.emit('jobQueued', { pid: processInfo.pid, jobId: message.jobId });
                break;

            case 'JOB_COMPLETE':
                console.log(`[Orchestrator] ✅ Job ${message.jobId} completed in ${processInfo.type}`);
                this._routeToNextStage(message);
                this.emit('jobComplete', { pid: processInfo.pid, jobId: message.jobId, result: message.result });
                break;

            case 'JOB_FAILED':
                console.log(`[Orchestrator] ❌ Job ${message.jobId} failed in ${processInfo.type}: ${message.error}`);
                this.emit('jobFailed', { pid: processInfo.pid, jobId: message.jobId, error: message.error });
                break;

            case 'SUBMIT_JOB':
                console.log(`[Orchestrator] 📨 Received SUBMIT_JOB from ${processInfo.type}`);
                this._handleJobSubmission(message);
                break;

            case 'SUBMITTER_STARTED':
                console.log(`[Orchestrator] 📤 Job submitter started: ${message.maxJobs} jobs`);
                this.emit('submitterStarted', message);
                break;

            case 'SUBMITTER_COMPLETE':
                console.log(`[Orchestrator] ✅ Job submitter completed: ${message.totalJobs} jobs`);
                this.emit('submitterComplete', message);
                break;

            case 'SQLITE_REQUEST':
                if (this.sqliteManager.isRunning()) {
                    this.sqliteManager.send(message.data);
                } else {
                    console.error('[Orchestrator] ❌ SQLite Server not available');
                }
                break;

            case 'SQLITE_RESPONSE':
                const { targetPid, ...response } = message;
                if (targetPid) {
                    this.processCreator.sendMessage(targetPid, response);
                }
                break;

            case 'STATUS':
                this.emit('status', { pid: processInfo.pid, ...message });
                break;

            case 'SHUTDOWN_COMPLETE':
                processInfo.status = 'stopped';
                this.emit('shutdownComplete', { pid: processInfo.pid });
                break;

            default:
                this.emit('message', { pid: processInfo.pid, message });
        }
    }

    // === Job Submission ===

    async submitJob(jobData) {
        const { type = 'browser', data, id } = jobData;
        
        const processInfo = await this.processCreator.waitForProcess(type);
        if (!processInfo) {
            throw new Error(`No running process of type: ${type} available`);
        }
        
        const jobId = id || data?.id || `${type}_${Date.now()}`;
        
        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Job submission timeout'));
            }, 10000);
            
            const listener = (msg) => {
                if (msg.type === 'JOB_QUEUED' && msg.jobId === jobId) {
                    clearTimeout(timeout);
                    processInfo.child.off('message', listener);
                    resolve({ jobId, status: 'queued' });
                }
                if (msg.type === 'JOB_ERROR' && msg.jobId === jobId) {
                    clearTimeout(timeout);
                    processInfo.child.off('message', listener);
                    reject(new Error(msg.error));
                }
            };
            
            processInfo.child.on('message', listener);
            processInfo.child.send({
                type: 'NEW_JOB',
                jobId,
                data: { ...data, id: jobId }
            });
        });
    }

    // === Handle Job Submission ===

    async _handleJobSubmission(message) {
        const { job, jobNumber, totalJobs, eventData } = message;
        
        if (this.isProcessingSubmission) {
            this.pendingSubmissions.push(message);
            return;
        }
        
        this.isProcessingSubmission = true;
        
        try {
            const analyzerProcess = await this.processCreator.waitForProcess('analyzer');
            if (!analyzerProcess) {
                console.error(`[Orchestrator] ❌ Analyzer not available for job ${jobNumber}`);
                this.emit('jobSubmissionError', { jobNumber, error: 'Analyzer not available' });
                return;
            }
            
            console.log(`[Orchestrator] 📤 Submitting job ${jobNumber}/${totalJobs}: ${job.id}`);
            if (eventData) {
                console.log(`[Orchestrator] 📋 ${eventData.EXCHANGE} - ${eventData.CONTRACT}`);
            }
            
            const result = await this.submitJob(job);
            console.log(`[Orchestrator] ✅ Job ${result.jobId} submitted (${jobNumber}/${totalJobs})`);
            this.emit('jobSubmitted', { jobNumber, totalJobs, jobId: result.jobId, eventData });
            
        } catch (error) {
            console.error(`[Orchestrator] ❌ Job ${jobNumber} failed:`, error.message);
            this.emit('jobSubmissionError', { jobNumber, error: error.message });
        } finally {
            this.isProcessingSubmission = false;
            this._processNextPending();
        }
    }

    _processNextPending() {
        if (this.pendingSubmissions.length > 0 && !this.isProcessingSubmission) {
            const next = this.pendingSubmissions.shift();
            this._handleJobSubmission(next);
        }
    }

    // === Route to Next Stage ===

    async _routeToNextStage(message) {
        const result = message.result;
        const currentType = message.processType || 'unknown';
        const jobId = message.jobId;
        
        // Map: current stage → next stage
        const stageMap = {
            'analyzer': 'browser',
            'browser': 'exporter',
            'exporter': null  // End of pipeline
        };
        
        // Special case: job-submitter → analyzer
        if (currentType === 'job-submitter' || currentType === 'submitter') {
            await this._routeToAnalyzer(message, result);
            return;
        }
        
        const nextType = stageMap[currentType];
        
        if (!nextType) {
            // Job completed all stages
            console.log(`[Orchestrator] 🎉 Job ${jobId} fully completed!`);
            this.emit('jobFullyComplete', { jobId, result });
            return;
        }

        console.log(`[Orchestrator] 🔄 Routing ${jobId} from ${currentType} to ${nextType}`);

        const nextJob = this._buildJobForStage(nextType, message, result);
        
        try {
            await this.submitJob(nextJob);
            console.log(`[Orchestrator] ✅ ${nextType} job created for ${jobId}`);
        } catch (error) {
            console.error(`[Orchestrator] ❌ Failed to route to ${nextType}:`, error.message);
        }
    }

    async _routeToAnalyzer(message, result) {
        const jobId = message.jobId;
        console.log(`[Orchestrator] 📤 Creating analyzer job for ${jobId}`);

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
            await this.submitJob(analyzerJob);
            console.log(`[Orchestrator] ✅ Analyzer job created for ${jobId}`);
        } catch (error) {
            console.error(`[Orchestrator] ❌ Failed to create analyzer job:`, error.message);
        }
    }

    _buildJobForStage(stage, message, result) {
        const jobId = message.jobId;
        
        switch (stage) {
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

    // === Start Job Submitter ===

    async startJobSubmitter(config = {}) {
        console.log('[Orchestrator] 🚀 Starting job submitter...');
        console.log(`[Orchestrator] 📋 Events: ${config.events?.length || 0}`);
        
        let submitterProcess = null;
        let attempts = 0;
        const maxAttempts = 5;
        
        while (attempts < maxAttempts) {
            submitterProcess = await this.processCreator.waitForProcess('job-submitter', 5000);
            if (submitterProcess) break;
            attempts++;
            console.log(`[Orchestrator] ⏳ Waiting for job-submitter (attempt ${attempts}/${maxAttempts})...`);
            await this._sleep(2000);
        }
        
        if (!submitterProcess) {
            console.error('[Orchestrator] ❌ Job submitter process not found!');
            return;
        }
        
        console.log(`[Orchestrator] 📨 Found job-submitter process: ${submitterProcess.pid}`);
        
        const message = {
            type: 'START_SUBMITTING',
            parentPid: process.pid,
            config: {
                maxJobs: config.maxJobs || config.events?.length || 10,
                submitInterval: config.submitInterval || 3000,
                events: config.events || []
            }
        };
        
        try {
            submitterProcess.child.send(message);
            console.log('[Orchestrator] ✅ Start signal sent successfully');
        } catch (error) {
            console.error('[Orchestrator] ❌ Failed to send START_SUBMITTING:', error.message);
        }
    }

    // === Heartbeat Monitor ===

    _startHeartbeatMonitor() {
        setInterval(() => {
            const now = Date.now();
            const processes = this.processCreator.getAllProcesses();
            
            for (const info of processes) {
                if (info.status === 'running') {
                    const elapsed = now - info.lastHeartbeat;
                    
                    if (elapsed > this.heartbeatTimeout) {
                        console.warn(`[Orchestrator] ⏰ Process ${info.pid} heartbeat timeout (${elapsed}ms)`);
                        this.emit('heartbeatTimeout', { pid: info.pid, info });
                        
                        if (this.isRunning) {
                            this.processCreator.restartProcess(info);
                        }
                    }
                }
            }
        }, 2000);
    }

    // === Stats ===

    async getProcessStats() {
        const stats = {};
        const processes = this.processCreator.getAllProcesses();
        for (const info of processes) {
            stats[info.pid] = {
                type: info.type,
                status: info.status,
                processingWorkers: info.processingWorkers,
                queueName: info.queueName,
                sqliteIndex: info.sqliteIndex,
                lastHeartbeat: info.lastHeartbeat,
                uptime: Date.now() - info.createdAt,
                restartCount: info.restartCount,
                isReady: info.isReady
            };
        }
        return stats;
    }

    // === Shutdown ===

    async shutdown() {
        console.log('[Orchestrator] 🛑 Shutting down...');
        this.isRunning = false;
        
        await this.processCreator.shutdown();
        await this.sqliteManager.shutdown();
        
        console.log('[Orchestrator] ✅ Shutdown complete');
        this.emit('shutdown');
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = Orchestrator;