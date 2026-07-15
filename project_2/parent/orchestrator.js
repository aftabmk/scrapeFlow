// parent/orchestrator.js
const { EventEmitter } = require('events');
const Message = require('../messaging/message');
const MessageRouter = require('../messaging/message-router');
const MessageHandler = require('../messaging/message-handler');
const Pipeline = require('../messaging/pipeline');
const { MessageTypes, MessageDestinations } = require('../messaging/message-types');
const ProcessCreator = require('./components/process-creator');
const SQLiteServerManager = require('./components/sqlite-server-manager');

class Orchestrator extends EventEmitter {
    constructor(options = {}) {
        super();

        this.isRunning = true;
        this.allProcessesReady = false;
        this.serverReady = false;
        this.readyChildren = 0;
        this.expectedChildren = 0;
        this.heartbeatTimeout = options.heartbeatTimeout || 15000;
        this.heartbeatCheckInterval = options.heartbeatCheckInterval || 2000;
        this.heartbeatTimer = null;
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

        // ✅ Message Handler (registered first)
        this.messageHandler = new MessageHandler();

        // ✅ Message Router (with MessageHandler injected)
        this.messageRouter = new MessageRouter({
            processManager: this.processCreator,
            sqliteManager: this.sqliteManager,
            orchestrator: this,
            messageHandler: this.messageHandler  // ✅ Inject MessageHandler
        });

        // ✅ Pipeline
        this.pipeline = new Pipeline({
            stages: options.pipeline || ['submitter', 'analyzer', 'browser', 'exporter'],
            orchestrator: this,
            processManager: this.processCreator,
            submitJobFn: (job) => this.submitJob(job)
        });

        // ✅ Setup everything
        this._setupMessageHandlers();
        this._setupListeners();
        this._startHeartbeatMonitor();

        console.log('[Orchestrator] ✅ Initialized');
        console.log(`[Orchestrator] 📋 Pipeline: ${this.pipeline.stages.join(' → ')}`);
    }

    // === Setup Message Handlers ===

    _setupMessageHandlers() {
        // ✅ Register all handlers with MessageHandler
        this.messageHandler
            .register(MessageTypes.READY, (msg) => {
                console.log(`[Orchestrator] ✅ Process ${msg.from} ready`);
                this.readyChildren++;
                this._checkAllReady();
            })
            .register(MessageTypes.HEARTBEAT, (msg) => {
                const processInfo = this.processCreator.getProcessByPid(msg.payload.pid);
                if (processInfo) {
                    processInfo.lastHeartbeat = Date.now();
                }
                this.emit('heartbeat', { pid: msg.payload.pid, stats: msg.payload.stats });
            })
            .register(MessageTypes.JOB_QUEUED, (msg) => {
                console.log(`[Orchestrator] 📝 Job ${msg.payload.jobId} queued in ${msg.from}`);
                this.emit('jobQueued', { jobId: msg.payload.jobId, from: msg.from });
            })
            .register(MessageTypes.JOB_COMPLETE, async (msg) => {
                console.log(`[Orchestrator] ✅ Job ${msg.payload.jobId} completed in ${msg.from}`);
                this.emit('jobComplete', { jobId: msg.payload.jobId, result: msg.payload.result, from: msg.from });
                await this.pipeline.advance(msg);
            })
            .register(MessageTypes.JOB_FAILED, (msg) => {
                console.log(`[Orchestrator] ❌ Job ${msg.payload.jobId} failed in ${msg.from}: ${msg.payload.error}`);
                this.emit('jobFailed', { jobId: msg.payload.jobId, error: msg.payload.error, from: msg.from });
            })
            .register(MessageTypes.SUBMIT_JOB, async (msg) => {
                console.log(`[Orchestrator] 📨 Received SUBMIT_JOB from ${msg.from}`);
                await this.pipeline.handleSubmitJob(msg);
            })
            .register(MessageTypes.SUBMITTER_STARTED, (msg) => {
                console.log(`[Orchestrator] 📤 Submitter started: ${msg.payload.maxJobs} jobs`);
                this.emit('submitterStarted', msg.payload);
            })
            .register(MessageTypes.SUBMITTER_COMPLETE, (msg) => {
                console.log(`[Orchestrator] ✅ Submitter completed: ${msg.payload.totalJobs} jobs`);
                this.emit('submitterComplete', msg.payload);
            })
            .register(MessageTypes.SHUTDOWN_COMPLETE, (msg) => {
                const processInfo = this.processCreator.getProcessByPid(msg.payload.pid);
                if (processInfo) {
                    processInfo.status = 'stopped';
                }
                this.emit('shutdownComplete', { pid: msg.payload.pid });
            })
            .register(MessageTypes.STATUS, (msg) => {
                this.emit('status', { from: msg.from, ...msg.payload });
            })
            .register(MessageTypes.ERROR, (msg) => {
                console.error(`[Orchestrator] ❌ Error from ${msg.from}:`, msg.payload.error);
                this.emit('error', { from: msg.from, error: msg.payload.error });
            })
            .register('JOB_SUBMITTED', (msg) => {
                console.log(`[Orchestrator] ✅ Job ${msg.payload.jobId} submitted (${msg.payload.jobNumber}/${msg.payload.totalJobs})`);
                this.emit('jobSubmitted', msg.payload);
            });

        // ✅ Pipeline event listeners
        this.pipeline.on('complete', ({ jobId, result }) => {
            console.log(`[Orchestrator] 🎉 Job ${jobId} fully completed!`);
            this.emit('jobFullyComplete', { jobId, result });
        });

        this.pipeline.on('error', ({ jobId, from, to, error }) => {
            console.error(`[Orchestrator] ❌ Pipeline error for ${jobId}:`, error.message);
            this.emit('pipelineError', { jobId, from, to, error });
        });

        this.pipeline.on('routed', ({ jobId, from, to }) => {
            console.log(`[Orchestrator] 🔄 Routed ${jobId}: ${from} → ${to}`);
        });

        this.pipeline.on('submitterStarted', (state) => {
            console.log(`[Orchestrator] 📤 Submitter started: ${state.maxJobs} jobs`);
        });

        // ✅ Fallback handler
        this.messageHandler.setFallback((msg) => {
            console.log(`[Orchestrator] 📨 Unhandled message: ${msg.type} from ${msg.from}`);
        });
    }

    // === Setup Event Listeners ===

    _setupListeners() {
        // ✅ ProcessCreator forwards messages to MessageRouter
        this.processCreator.on('message', (processInfo, raw) => {
            const msg = Message.from(raw);
            this.messageRouter.route(msg);
        });

        this.processCreator.on('processReady', (data) => {
            console.log(`[Orchestrator] ✅ Process ${data.pid} (${data.type}) ready`);
            this.readyChildren++;
            this._checkAllReady();
        });

        this.processCreator.on('processTimeout', (data) => {
            this.emit('processTimeout', data);
        });

        this.processCreator.on('exit', (processInfo, code, signal) => {
            console.log(`[Orchestrator] ⚠️ Process ${processInfo.pid} (${processInfo.type}) exited with code ${code}`);
            this.readyChildren--;
            this.allProcessesReady = false;
            this.emit('processExit', { pid: processInfo.pid, type: processInfo.type, code, signal });
        });

        // ✅ SQLiteManager events
        this.sqliteManager.on('ready', () => {
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

    // === Start SQLite Server ===

    async startSQLiteServer(options = {}) {
        return this.sqliteManager.start();
    }

    // === Process Creation ===

    async createProcess(options = {}) {
        const { type } = options;
        this.expectedChildren++;
        return this.processCreator.createProcess({
            ...options,
            serverReady: this.serverReady
        });
    }

    async createAllProcesses(processConfigs) {
        const promises = processConfigs.map(config => this.createProcess(config));
        return Promise.all(promises);
    }

    // === Check All Ready ===

    // parent/orchestrator.js - In _checkAllReady()

    _checkAllReady() {
        const allReady = this.readyChildren === this.expectedChildren && this.expectedChildren > 0;
        const serverReady = this.serverReady;

        if (allReady && serverReady && !this.allProcessesReady) {
            this.allProcessesReady = true;
            console.log('[Orchestrator] 🎯 All processes are ready!');

            // ✅ Debug: Log all processes
            const processes = this.processCreator.getProcesses();
            console.log('[Orchestrator] 📋 Process states:');
            for (const [pid, info] of Object.entries(processes)) {
                console.log(`[Orchestrator]   ${pid}: ${info.type} (status=${info.status}, isReady=${info.isReady})`);
            }

            this.emit('allProcessesReady');
        }
    }

    // === Heartbeat Monitor ===

    _startHeartbeatMonitor() {
        console.log(`[Orchestrator] 💓 Starting heartbeat monitor (timeout: ${this.heartbeatTimeout}ms, interval: ${this.heartbeatCheckInterval}ms)`);

        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }

        this.heartbeatTimer = setInterval(() => {
            this._checkHeartbeats();
        }, this.heartbeatCheckInterval);
    }

    _checkHeartbeats() {
        const now = Date.now();
        const processes = this.processCreator.getAllProcesses();

        for (const info of processes) {
            if (info.status !== 'running' || !info.isReady) continue;

            const elapsed = now - info.lastHeartbeat;

            if (elapsed > this.heartbeatTimeout) {
                console.warn(`[Orchestrator] ⏰ Process ${info.pid} (${info.type}) heartbeat timeout!`);
                console.warn(`[Orchestrator]    Elapsed: ${elapsed}ms (threshold: ${this.heartbeatTimeout}ms)`);

                this.emit('heartbeatTimeout', {
                    pid: info.pid,
                    type: info.type,
                    elapsed: elapsed,
                    lastHeartbeat: info.lastHeartbeat
                });

                if (this.isRunning) {
                    this._restartProcess(info);
                }
            }
        }
    }

    async _restartProcess(processInfo) {
        console.log(`[Orchestrator] 🔄 Restarting process ${processInfo.pid} (${processInfo.type})...`);

        try {
            const newProcess = await this.processCreator.restartProcess(processInfo);
            console.log(`[Orchestrator] ✅ Process ${processInfo.pid} restarted as ${newProcess.pid}`);
            this.emit('processRestarted', {
                oldPid: processInfo.pid,
                newPid: newProcess.pid,
                type: processInfo.type
            });
        } catch (error) {
            console.error(`[Orchestrator] ❌ Failed to restart process ${processInfo.pid}:`, error.message);
            this.emit('processRestartFailed', {
                pid: processInfo.pid,
                type: processInfo.type,
                error: error.message
            });
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

        const msg = new Message({
            from: MessageDestinations.ORCHESTRATOR,
            to: type,
            type: MessageTypes.NEW_JOB,
            payload: {
                jobId,
                data: { ...data, id: jobId }
            }
        });

        return new Promise((resolve, reject) => {
            const timeout = setTimeout(() => {
                reject(new Error('Job submission timeout'));
            }, 10000);

            const listener = (raw) => {
                const response = Message.from(raw);
                if (response.type === MessageTypes.JOB_QUEUED && response.payload.jobId === jobId) {
                    clearTimeout(timeout);
                    processInfo.child.off('message', listener);
                    resolve({ jobId, status: 'queued' });
                }
                if (response.type === MessageTypes.ERROR && response.payload.jobId === jobId) {
                    clearTimeout(timeout);
                    processInfo.child.off('message', listener);
                    reject(new Error(response.payload.error));
                }
            };

            processInfo.child.on('message', listener);
            msg.send(processInfo.child);
        });
    }

    // === Start Pipeline ===

    async startPipeline(config = {}) {
        console.log('[Orchestrator] 🚀 Starting pipeline...');
        await this.pipeline.start(config);
        this.emit('pipelineStarted', { events: config.events || [] });
    }

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
                heartbeatAge: Date.now() - info.lastHeartbeat,
                uptime: Date.now() - info.createdAt,
                restartCount: info.restartCount,
                isReady: info.isReady
            };
        }
        return stats;
    }

    async getPipelineStatus() {
        return this.pipeline.getStatus();
    }

    async getHeartbeatStats() {
        const processes = this.processCreator.getAllProcesses();
        const stats = {
            total: processes.length,
            healthy: 0,
            unhealthy: 0,
            details: []
        };

        const now = Date.now();
        for (const info of processes) {
            const elapsed = now - info.lastHeartbeat;
            const isHealthy = elapsed < this.heartbeatTimeout;

            if (isHealthy) {
                stats.healthy++;
            } else {
                stats.unhealthy++;
            }

            stats.details.push({
                pid: info.pid,
                type: info.type,
                status: info.status,
                isReady: info.isReady,
                lastHeartbeat: info.lastHeartbeat,
                heartbeatAge: elapsed,
                isHealthy
            });
        }

        return stats;
    }

    // === Shutdown ===

    async shutdown() {
        console.log('[Orchestrator] 🛑 Shutting down...');
        this.isRunning = false;

        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
            console.log('[Orchestrator] 💓 Heartbeat monitor stopped');
        }

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