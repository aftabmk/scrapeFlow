// parent/orchestrator.js
const { EventEmitter } = require('events');
const ProcessManager = require('./components/process-manager');
const MessageRouter = require('./components/message-router');
const JobRouter = require('./components/job-router');
const HealthMonitor = require('./components/health-monitor');
const SQLiteServerManager = require('./components/sqlite-server-manager');

class Orchestrator extends EventEmitter {
    constructor(options = {}) {
        super();
        this.isRunning = true;
        this.allProcessesReady = false;
        this.options = options;

        // ✅ Initialize components
        this.processManager = new ProcessManager({
            restartDelay: options.restartDelay || 2000
        });

        this.jobRouter = new JobRouter({
            processManager: this.processManager,
            pipeline: options.pipeline || ['analyzer', 'browser', 'exporter']
        });

        this.healthMonitor = new HealthMonitor({
            processManager: this.processManager,
            heartbeatTimeout: options.heartbeatTimeout || 15000,
            checkInterval: options.healthCheckInterval || 2000,
            onRestart: (info) => {
                this.processManager.restartProcess(info);
            }
        });

        this.sqliteManager = new SQLiteServerManager({
            dbPath: options.dbPath || './data/queue.db',
            readWorkers: options.readWorkers || 3,
            writeWorkers: options.writeWorkers || 1,
            startTimeout: options.sqliteTimeout || 30000,
            restartDelay: options.restartDelay || 2000,
            maxAttempts: options.sqliteMaxAttempts || 3
        });

        this.messageRouter = new MessageRouter();

        // ✅ Setup job router submit function
        this.jobRouter.setSubmitJobFn((job) => this.submitJob(job));

        // ✅ Setup message handlers
        this._setupMessageHandlers();
        this._setupEventListeners();
    }

    // === Setup Message Handlers ===
    // parent/orchestrator.js - Updated message handler registration

    _setupMessageHandlers() {
        // Register handlers for each message type
        this.messageRouter
            .register('HEARTBEAT', (msg, processInfo) => {
                this.healthMonitor.updateHeartbeat(processInfo.pid, msg.stats);
                this.emit('heartbeat', { pid: processInfo.pid, stats: msg.stats });
            })
            .register('JOB_QUEUED', (msg, processInfo) => {
                console.log(`[Orchestrator] 📝 Job ${msg.jobId} queued in ${processInfo.type}`);
                this.emit('jobQueued', { pid: processInfo.pid, jobId: msg.jobId });
            })
            .register('JOB_COMPLETE', (msg, processInfo) => {
                console.log(`[Orchestrator] ✅ Job ${msg.jobId} completed in ${processInfo.type}`);
                msg.processType = processInfo.type;
                this.jobRouter.routeToNext(msg);
                this.emit('jobComplete', { pid: processInfo.pid, jobId: msg.jobId, result: msg.result });
            })

            .register('JOB_FAILED', (msg, processInfo) => {
                console.log(`[Orchestrator] ❌ Job ${msg.jobId} failed in ${processInfo.type}: ${msg.error}`);
                this.emit('jobFailed', { pid: processInfo.pid, jobId: msg.jobId, error: msg.error });
            })
            .register('SUBMIT_JOB', (msg, processInfo) => {
                console.log(`[Orchestrator] 📨 Received SUBMIT_JOB from ${processInfo.type}`);
                this.jobRouter.handleJobSubmission(msg);
            })
            .register('SUBMITTER_STARTED', (msg, processInfo) => {
                console.log(`[Orchestrator] 📤 Job submitter started: ${msg.maxJobs} jobs`);
                this.emit('submitterStarted', msg);
            })
            .register('SUBMITTER_COMPLETE', (msg, processInfo) => {
                console.log(`[Orchestrator] ✅ Job submitter completed: ${msg.totalJobs} jobs`);
                this.emit('submitterComplete', msg);
            })
            .register('SQLITE_REQUEST', (msg, processInfo) => {
                // ✅ Fix: isRunning is a method, not a property
                if (this.sqliteManager.isRunning()) {
                    console.log(`[Orchestrator] 🔄 Forwarding SQLITE_REQUEST to SQLite Server`);
                    this.sqliteManager.send(msg.data);
                } else {
                    console.error('[Orchestrator] ❌ SQLite Server not available for request');
                    // Send error response back to child
                    if (msg.data && msg.data.jobId && msg.data.sourcePid) {
                        const errorResponse = {
                            type: 'SQLITE_RESPONSE',
                            targetPid: msg.data.sourcePid,
                            jobId: msg.data.jobId,
                            error: 'SQLite Server not available'
                        };
                        this.processManager.sendMessage(msg.data.sourcePid, errorResponse);
                    }
                }
            })
            .register('SQLITE_RESPONSE', (msg, processInfo) => {
                const { targetPid, ...response } = msg;
                if (targetPid) {
                    this.processManager.sendMessage(targetPid, response);
                }
            })
            .register('STATUS', (msg, processInfo) => {
                this.emit('status', { pid: processInfo.pid, ...msg });
            })
            .register('SHUTDOWN_COMPLETE', (msg, processInfo) => {
                processInfo.status = 'stopped';
                this.emit('shutdownComplete', { pid: processInfo.pid });
            })
            .setDefaultHandler((msg, processInfo) => {
                console.log(`[Orchestrator] 📨 Unhandled message type: ${msg.type} from ${processInfo.type}`);
                this.emit('unhandledMessage', { pid: processInfo.pid, message: msg });
            });
    }

    // === Setup Event Listeners ===

    _setupEventListeners() {
        // ProcessManager events
        this.processManager.on('processReady', (data) => {
            this.emit('processReady', data);
            this._checkAllReady();
        });

        this.processManager.on('processTimeout', (data) => {
            this.emit('processTimeout', data);
        });

        this.processManager.on('exit', (processInfo, code, signal) => {
            console.log(`[Orchestrator] ⚠️ Process ${processInfo.pid} (${processInfo.type}) exited with code ${code}`);
            this.emit('processExit', { pid: processInfo.pid, type: processInfo.type, code, signal });
            this.allProcessesReady = false;
        });

        this.processManager.on('message', (processInfo, message) => {
            this.messageRouter.route(message, processInfo);
        });

        // HealthMonitor events
        this.healthMonitor.on('heartbeat', (data) => {
            this.emit('healthHeartbeat', data);
        });

        this.healthMonitor.on('missed', (data) => {
            this.emit('healthMissed', data);
        });

        // SQLiteManager events
        this.sqliteManager.on('ready', (info) => {
            console.log('[Orchestrator] ✅ SQLite Server ready');
            this.emit('sqliteReady', info);
        });

        this.sqliteManager.on('maxAttemptsReached', (data) => {
            console.error('[Orchestrator] ❌ SQLite Server max restart attempts reached');
            this.emit('sqliteMaxAttempts', data);
        });

        // JobRouter events
        this.jobRouter.on('jobComplete', (data) => {
            console.log(`[Orchestrator] 🎉 Job ${data.jobId} fully completed!`);
            this.emit('jobFullyComplete', data);
        });

        this.jobRouter.on('submitted', (data) => {
            this.emit('jobSubmitted', data);
        });

        this.jobRouter.on('error', (data) => {
            this.emit('jobSubmissionError', data);
        });
    }

    // === Public API ===

    async startSQLiteServer(options = {}) {
        if (options.dbPath) this.sqliteManager.dbPath = options.dbPath;
        if (options.readWorkers) this.sqliteManager.readWorkers = options.readWorkers;
        if (options.writeWorkers) this.sqliteManager.writeWorkers = options.writeWorkers;
        return this.sqliteManager.start();
    }

    async createProcess(options) {
        return this.processManager.createProcess(options);
    }

    async submitJob(jobData) {
        const { type = 'browser', data, id } = jobData;

        const processInfo = await this.processManager.waitForProcess(type);
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

    async startJobSubmitter(config = {}) {
        console.log('[Orchestrator] 🚀 Starting job submitter...');
        console.log(`[Orchestrator] 📋 Events: ${config.events?.length || 0}`);

        let submitterProcess = null;
        let attempts = 0;
        const maxAttempts = 5;

        while (attempts < maxAttempts) {
            submitterProcess = await this.processManager.waitForProcess('job-submitter', 5000);
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

    async getProcessStats() {
        const stats = {};
        const processes = this.processManager.getAllProcesses();
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

    async getHealthStatus() {
        return this.healthMonitor.getHealthStatus();
    }

    _checkAllReady() {
        if (this.allProcessesReady) return;
        if (this.processManager.isAllReady()) {
            this.allProcessesReady = true;
            console.log('[Orchestrator] 🎯 All processes are ready!');
            this.emit('allProcessesReady');
        }
    }

    async shutdown() {
        console.log('[Orchestrator] 🛑 Shutting down...');
        this.isRunning = false;

        this.healthMonitor.stop();
        await this.processManager.shutdown();
        await this.sqliteManager.shutdown();

        console.log('[Orchestrator] ✅ Shutdown complete');
        this.emit('shutdown');
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = Orchestrator;