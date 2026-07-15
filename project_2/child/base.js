// child/base.js
const { EventEmitter } = require('events');
const Message = require('../messaging/message');
const { MessageTypes, MessageDestinations } = require('../messaging/message-types');
const DurableQueue = require('../queue/DurableQueue');
const WriteQueue = require('../queue/WriteQueue');
const SQLiteCommWorker = require('../workers/sqlite-comm-worker');

class BaseChildProcess extends EventEmitter {
    constructor(options = {}) {
        super();

        const args = this._parseArgs();

        this.processType = options.processType || args.processType || 'generic';
        this.queueName = options.queueName || args.queueName || `${this.processType}_queue`;
        this.processingWorkers = options.processingWorkers || parseInt(args.processingWorkers) || 2;
        this.dbPath = options.dbPath || './data/queue.db';
        this.serverReady = options.serverReady === true || args.serverReady === 'true' || false;
        this.isRunning = true;
        this.messageHandlers = new Map();

        console.log(`[${this.processType}] 🟢 Starting with ${this.processingWorkers} workers`);
        console.log(`[${this.processType}] 📋 Queue: ${this.queueName}`);
        console.log(`[${this.processType}] 📋 ServerReady: ${this.serverReady}`);

        this.writeQueue = new WriteQueue();

        this.sqliteCommWorker = new SQLiteCommWorker({
            workerId: `sqlite_comm_${process.pid}`,
            writeQueue: this.writeQueue,
            queueName: this.queueName,
            pollInterval: 100,
            batchSize: 50
        });

        this.queue = new DurableQueue(this.queueName, {
            visibilityTimeout: 30000,
            maxRetries: 5,
            writeQueue: this.writeQueue,
            commWorker: this.sqliteCommWorker,
            serverReady: this.serverReady
        });

        this.stats = {
            jobsProcessed: 0,
            jobsFailed: 0,
            startTime: Date.now()
        };

        this._init();
    }

    _init() {
        this._setupMessageHandlers();
        this._setupWorkerManager();
        this._startHeartbeat();

        const msg = new Message({
            from: this.processType,
            to: MessageDestinations.ORCHESTRATOR,
            type: MessageTypes.READY,
            payload: {
                processType: this.processType,
                pid: process.pid,
                processingWorkers: this.processingWorkers,
                queueName: this.queueName
            }
        });
        msg.send();
    }

    _parseArgs() {
        const args = {};
        for (let i = 2; i < process.argv.length; i++) {
            const arg = process.argv[i];
            if (arg.startsWith('--')) {
                const [key, value] = arg.slice(2).split('=');
                args[key] = value || true;
            }
        }
        return args;
    }

    _setupMessageHandlers() {
        process.on('message', (raw) => {
            const msg = Message.from(raw);

            if (msg.to !== this.processType && msg.to !== MessageDestinations.ALL) {
                return;
            }

            const handler = this.messageHandlers.get(msg.type);
            if (handler) {
                try {
                    handler(msg);
                } catch (error) {
                    console.error(`[${this.processType}] ❌ Handler failed:`, error.message);
                }
            } else {
                console.log(`[${this.processType}] 📨 Unhandled message: ${msg.type}`);
            }
        });

        this.messageHandlers.set(MessageTypes.NEW_JOB, (msg) => this._handleNewJob(msg));
        this.messageHandlers.set(MessageTypes.SHUTDOWN, (msg) => this.shutdown());
        this.messageHandlers.set(MessageTypes.GET_STATUS, (msg) => this._sendStatus());
    }

    async _handleNewJob(msg) {
        try {
            const jobData = {
                id: msg.payload.jobId,
                data: msg.payload.data
            };

            const jobId = await this.queue.enqueue(jobData);
            console.log(`[${this.processType}] 📝 Job ${jobId} enqueued`);

            const response = new Message({
                from: this.processType,
                to: MessageDestinations.ORCHESTRATOR,
                type: MessageTypes.JOB_QUEUED,
                payload: {
                    jobId,
                    timestamp: Date.now()
                },
                correlationId: msg.requestId
            });
            response.send();
        } catch (error) {
            console.error(`[${this.processType}] ❌ Failed to enqueue job:`, error.message);
            const errorMsg = new Message({
                from: this.processType,
                to: MessageDestinations.ORCHESTRATOR,
                type: MessageTypes.ERROR,
                payload: {
                    jobId: msg.payload.jobId,
                    error: error.message
                },
                correlationId: msg.requestId
            });
            errorMsg.send();
        }
    }

    _setupWorkerManager() {
        if (this.processingWorkers === 0) {
            console.log(`[${this.processType}] ⏳ No workers`);
            return;
        }

        this.workers = [];
        for (let i = 0; i < this.processingWorkers; i++) {
            this._startWorker(i);
        }
    }

    async _startWorker(workerId) {
        console.log(`[${this.processType}] 👷 Worker ${workerId} started`);
        let emptyCount = 0;

        while (this.isRunning) {
            try {
                const job = await this.queue.dequeue();

                if (!job) {
                    emptyCount++;
                    if (emptyCount % 50 === 0) {
                        console.log(`[${this.processType}] ⏳ Worker ${workerId} waiting for jobs...`);
                    }
                    await this._sleep(500);
                    continue;
                }

                emptyCount = 0;
                console.log(`[${this.processType}] 🔄 Worker ${workerId} processing ${job.id}`);

                try {
                    const result = await this._processJob(job);
                    await this.queue.ack(job.id);
                    this.stats.jobsProcessed++;
                    console.log(`[${this.processType}] ✅ Worker ${workerId} completed ${job.id}`);

                    const completeMsg = new Message({
                        from: this.processType,
                        to: MessageDestinations.ORCHESTRATOR,
                        type: MessageTypes.JOB_COMPLETE,
                        payload: {
                            jobId: job.id,
                            result,
                            timestamp: Date.now()
                        }
                    });
                    completeMsg.send();

                } catch (error) {
                    this.stats.jobsFailed++;
                    console.error(`[${this.processType}] ❌ Worker ${workerId} failed ${job.id}:`, error.message);

                    const failMsg = new Message({
                        from: this.processType,
                        to: MessageDestinations.ORCHESTRATOR,
                        type: MessageTypes.JOB_FAILED,
                        payload: {
                            jobId: job.id,
                            error: error.message,
                            timestamp: Date.now()
                        }
                    });
                    failMsg.send();
                }

            } catch (error) {
                console.error(`[${this.processType}] Worker ${workerId} loop error:`, error);
                await this._sleep(1000);
            }
        }
    }

    async _processJob(job) {
        await this._sleep(1000);
        return {
            jobId: job.id,
            processedAt: new Date().toISOString(),
            data: job.data,
            result: `Processed by ${this.processType}`
        };
    }

    _startHeartbeat() {
        setInterval(() => {
            if (!this.isRunning) return;

            const msg = new Message({
                from: this.processType,
                to: MessageDestinations.ORCHESTRATOR,
                type: MessageTypes.HEARTBEAT,
                payload: {
                    pid: process.pid,
                    timestamp: Date.now(),
                    stats: {
                        activeJobs: this.queue.getInFlightCount(),
                        workers: this.processingWorkers,
                        queueName: this.queueName,
                        queueSize: this.queue.queue.getSize(),
                        writeQueueSize: this.queue.getWriteQueueSize(),
                        jobsProcessed: this.stats.jobsProcessed,
                        jobsFailed: this.stats.jobsFailed,
                        uptime: Date.now() - this.stats.startTime
                    }
                }
            });
            msg.send();
        }, 5000);
    }

    _sendStatus() {
        const msg = new Message({
            from: this.processType,
            to: MessageDestinations.ORCHESTRATOR,
            type: MessageTypes.STATUS,
            payload: {
                activeJobs: this.queue.getInFlightCount(),
                queueSize: this.queue.queue.getSize(),
                writeQueueSize: this.queue.getWriteQueueSize(),
                jobsProcessed: this.stats.jobsProcessed,
                jobsFailed: this.stats.jobsFailed,
                pid: process.pid
            }
        });
        msg.send();
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async shutdown() {
        console.log(`[${this.processType}] 🛑 Shutting down...`);
        this.isRunning = false;

        if (this.sqliteCommWorker) {
            try {
                this.sqliteCommWorker.shutdown();
            } catch (error) {
                console.error(`[${this.processType}] Error shutting down SQLiteCommWorker:`, error.message);
            }
        }

        if (this.queue) {
            try {
                this.queue.shutdown();
            } catch (error) {
                console.error(`[${this.processType}] Error shutting down queue:`, error.message);
            }
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

module.exports = BaseChildProcess;