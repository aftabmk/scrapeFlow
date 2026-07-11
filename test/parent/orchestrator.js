// parent/orchestrator.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class Orchestrator extends EventEmitter {
    constructor(options = {}) {
        super();
        this.processes = new Map();
        this.heartbeatTimeout = options.heartbeatTimeout || 15000;
        this.restartDelay = options.restartDelay || 2000;
        this.isRunning = true;
        this.allProcessesReady = false;
        this.pendingSubmissions = [];
        this.isProcessingSubmission = false;
        this.expectedProcesses = 0;
        this.readyProcesses = 0;
        this.sqliteServer = null;
        
        this._startHeartbeatMonitor();
    }

    // === Process Management ===

    createProcess(options = {}) {
        const {
            type = 'browser',
            processingWorkers = 2,
            queueName = `${type}_queue`,
            args = []
        } = options;

        const scriptMap = {
            browser: './child/browser.js',
            analyzer: './child/analyzer.js',
            exporter: './child/exporter.js',
            'job-submitter': './child/job-submitter.js'
        };
        
        const scriptPath = scriptMap[type] || scriptMap.browser;
        const fullPath = path.join(__dirname, '..', scriptPath);

        const child = fork(fullPath, [
            `--processing-workers=${processingWorkers}`,
            `--queue-name=${queueName}`,
            `--process-type=${type}`,
            ...args
        ], {
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
            execArgv: ['--experimental-sqlite']
        });

        const processInfo = {
            pid: child.pid,
            child,
            type,
            processingWorkers,
            queueName,
            lastHeartbeat: Date.now(),
            status: 'starting',
            createdAt: Date.now(),
            restartCount: 0,
            isReady: false
        };

        child.on('message', (msg) => {
            this._handleChildMessage(processInfo, msg);
        });

        child.on('exit', (code, signal) => {
            this._handleChildExit(processInfo, code, signal);
        });

        child.on('error', (err) => {
            console.error(`[Orchestrator] Child ${child.pid} error:`, err);
            this._handleChildExit(processInfo, -1, 'error');
        });

        this.processes.set(child.pid, processInfo);
        this.expectedProcesses++;

        return new Promise((resolve) => {
            const readyHandler = (msg) => {
                if (msg.type === 'ready' && msg.processType === type) {
                    processInfo.status = 'running';
                    processInfo.isReady = true;
                    this.readyProcesses++;
                    
                    console.log(`[Orchestrator] ✅ Process ${child.pid} (${type}) ready (${this.readyProcesses}/${this.expectedProcesses})`);
                    this.emit('processReady', { pid: child.pid, type, ...msg });
                    
                    this._checkAllProcessesReady();
                    
                    resolve(processInfo);
                    child.off('message', readyHandler);
                }
            };
            
            child.on('message', readyHandler);
            
            setTimeout(() => {
                child.off('message', readyHandler);
                if (processInfo.status === 'starting') {
                    processInfo.status = 'timeout';
                    console.warn(`[Orchestrator] Process ${child.pid} (${type}) startup timeout`);
                    this.emit('processTimeout', { pid: child.pid, type });
                    resolve(processInfo);
                }
            }, 15000);
        });
    }

    _checkAllProcessesReady() {
        if (this.allProcessesReady) return;
        
        if (this.readyProcesses === this.expectedProcesses && this.expectedProcesses > 0) {
            this.allProcessesReady = true;
            console.log(`[Orchestrator] 🎯 All ${this.expectedProcesses} processes are ready!`);
            
            setTimeout(() => {
                this.emit('allProcessesReady');
            }, 1000);
        }
    }

    async _waitForProcess(type, timeout = 30000) {
        const startTime = Date.now();
        
        while (Date.now() - startTime < timeout) {
            for (const [pid, info] of this.processes) {
                if (info.type === type && info.status === 'running' && info.isReady) {
                    return info;
                }
            }
            await this._sleep(500);
        }
        return null;
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
                
                if (processInfo.type === 'analyzer') {
                    this._routeToBrowser(message);
                } else if (processInfo.type === 'browser') {
                    this._routeToExporter(message);
                } else if (processInfo.type === 'exporter') {
                    console.log(`[Orchestrator] 🎉 Job ${message.jobId} fully completed!`);
                    this.emit('jobFullyComplete', { pid: processInfo.pid, jobId: message.jobId, result: message.result });
                }
                
                this.emit('jobComplete', { pid: processInfo.pid, jobId: message.jobId, result: message.result });
                break;
                
            case 'JOB_FAILED':
                console.log(`[Orchestrator] ❌ Job ${message.jobId} failed in ${processInfo.type}: ${message.error}`);
                this.emit('jobFailed', { pid: processInfo.pid, jobId: message.jobId, error: message.error });
                break;
                
            case 'SUBMIT_JOB':
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
                // ✅ Forward SQLite request to SQLite Server
                if (this.sqliteServer) {
                    this.sqliteServer.send(message.data);
                }
                break;

            case 'SQLITE_RESPONSE':
                // ✅ Forward SQLite response to child
                const { targetPid, ...response } = message;
                if (this.processes.has(targetPid)) {
                    this.processes.get(targetPid).child.send(response);
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
        
        const processInfo = await this._waitForProcess(type);
        if (!processInfo) {
            throw new Error(`No running process of type: ${type} available`);
        }
        
        // ✅ Use the ID from the job (exchange-contract)
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
                jobId,  // ✅ exchange-contract
                data: { ...data, id: jobId }
            });
        });
    }

    // === Job Submission Handler ===

    async _handleJobSubmission(message) {
        const { job, jobNumber, totalJobs, eventData } = message;
        
        if (this.isProcessingSubmission) {
            this.pendingSubmissions.push(message);
            return;
        }
        
        this.isProcessingSubmission = true;
        
        try {
            const analyzerProcess = await this._waitForProcess('analyzer');
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

    // === Routing ===

    async _routeToBrowser(message) {
        const result = message.result;
        
        const browserProcess = await this._waitForProcess('browser');
        if (!browserProcess) {
            console.error(`[Orchestrator] ❌ Browser not available`);
            return;
        }
        
        // ✅ Use the same ID for browser job
        const jobId = message.jobId;
        console.log(`[Orchestrator] 🔄 Routing to browser: ${jobId}`);
        
        const browserJob = {
            id: jobId,  // ✅ Same ID
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
                analyzedAt: result.analyzedAt
            }
        };
        
        try {
            await this.submitJob(browserJob);
            console.log(`[Orchestrator] ✅ Browser job created for ${jobId}`);
        } catch (error) {
            console.error(`[Orchestrator] ❌ Failed to route to browser:`, error.message);
        }
    }

    async _routeToExporter(message) {
        const result = message.result;
        
        const exporterProcess = await this._waitForProcess('exporter');
        if (!exporterProcess) {
            console.error(`[Orchestrator] ❌ Exporter not available`);
            return;
        }
        
        // ✅ Use the same ID for exporter job
        const jobId = message.jobId;
        console.log(`[Orchestrator] 🔄 Routing to exporter: ${jobId}`);
        
        const exporterJob = {
            id: jobId,  // ✅ Same ID
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
                scrapedAt: result.scrapedAt || new Date().toISOString()
            }
        };
        
        try {
            await this.submitJob(exporterJob);
            console.log(`[Orchestrator] ✅ Exporter job created for ${jobId}`);
        } catch (error) {
            console.error(`[Orchestrator] ❌ Failed to route to exporter:`, error.message);
        }
    }

    // === Process Exit & Restart ===

    _handleChildExit(processInfo, code, signal) {
        processInfo.status = 'exited';
        processInfo.exitCode = code;
        processInfo.exitSignal = signal;
        processInfo.isReady = false;
        
        console.log(`[Orchestrator] ⚠️ Process ${processInfo.pid} (${processInfo.type}) exited with code ${code}`);
        
        this.emit('processExit', { 
            pid: processInfo.pid, 
            type: processInfo.type,
            code, 
            signal 
        });

        this.allProcessesReady = false;
        this.readyProcesses--;

        if (this.isRunning && code !== 0) {
            this._restartProcess(processInfo);
        }
    }

    async _restartProcess(processInfo) {
        const { type, processingWorkers, queueName } = processInfo;
        
        console.log(`[Orchestrator] 🔄 Restarting process ${processInfo.pid} (${type})...`);
        
        this.processes.delete(processInfo.pid);
        this.expectedProcesses--;
        
        await this._sleep(this.restartDelay);
        
        const newProcess = await this.createProcess({
            type,
            processingWorkers,
            queueName
        });
        
        console.log(`[Orchestrator] ✅ Process ${processInfo.pid} restarted as ${newProcess.pid}`);
    }

    // === Heartbeat Monitor ===

    _startHeartbeatMonitor() {
        setInterval(() => {
            const now = Date.now();
            
            for (const [pid, info] of this.processes) {
                if (info.status === 'running') {
                    const elapsed = now - info.lastHeartbeat;
                    
                    if (elapsed > this.heartbeatTimeout) {
                        console.warn(`[Orchestrator] ⏰ Process ${pid} heartbeat timeout (${elapsed}ms)`);
                        this.emit('heartbeatTimeout', { pid, info });
                        
                        if (this.isRunning) {
                            this._restartProcess(info);
                        }
                    }
                }
            }
        }, 2000);
    }

    // === Start Job Submitter ===

    async startJobSubmitter(config = {}) {
        console.log('[Orchestrator] 🚀 Starting job submitter...');
        console.log(`[Orchestrator] 📋 Events: ${config.events?.length || 0}`);
        
        const submitterProcess = await this._waitForProcess('job-submitter');
        if (!submitterProcess) {
            console.error('[Orchestrator] ❌ Job submitter not found!');
            return;
        }
        
        console.log(`[Orchestrator] 📨 Sending START_SUBMITTING to process ${submitterProcess.pid}`);
        
        submitterProcess.child.send({
            type: 'START_SUBMITTING',
            parentPid: process.pid,
            config: {
                maxJobs: config.maxJobs || config.events?.length || 10,
                submitInterval: config.submitInterval || 300,
                events: config.events || []
            }
        });
        
        console.log('[Orchestrator] ✅ Start signal sent');
    }

    // === SQLite Server ===

    setSQLiteServer(sqliteServer) {
        this.sqliteServer = sqliteServer;
    }

    // === Stats ===

    async getProcessStats() {
        const stats = {};
        for (const [pid, info] of this.processes) {
            stats[pid] = {
                type: info.type,
                status: info.status,
                processingWorkers: info.processingWorkers,
                queueName: info.queueName,
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
        
        const promises = [];
        for (const [pid, info] of this.processes) {
            promises.push(new Promise((resolve) => {
                try {
                    info.child.send({ type: 'SHUTDOWN' });
                } catch (err) {
                    console.log({message : err.message});
                }
                info.child.on('exit', resolve);
                setTimeout(resolve, 3000);
            }));
        }
        
        await Promise.all(promises);
        this.processes.clear();
        
        console.log('[Orchestrator] ✅ Shutdown complete');
        this.emit('shutdown');
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = Orchestrator;