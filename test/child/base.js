// child/base.js
const { EventEmitter } = require('events');
const DurableQueue = require('../queue/DurableQueue');
const WriteQueue = require('../queue/WriteQueue');
const SQLiteCommWorker = require('../workers/sqlite-comm-worker');

class BaseChildProcess extends EventEmitter {
    constructor(options = {}) {
        super();

        const args = this._parseArgs();

        // Core properties
        this.processType = options.processType || args.processType || 'generic';
        this.queueName = options.queueName || args.queueName || `${this.processType}_queue`;
        this.processingWorkers = options.processingWorkers || parseInt(args.processingWorkers) || 2;
        this.dbPath = options.dbPath || './data/queue.db';
        
        // ✅ ServerReady flag from parent
        this.serverReady = options.serverReady === true || args.serverReady === 'true' || false;
        
        this.isRunning = true;

        console.log(`[${this.processType}] 🟢 Starting with ${this.processingWorkers} workers`);
        console.log(`[${this.processType}] 📋 Queue: ${this.queueName}`);
        console.log(`[${this.processType}] 📋 ServerReady: ${this.serverReady}`);

        // ✅ Write Queue
        this.writeQueue = new WriteQueue();

        // ✅ SQLite Comm Worker
        this.sqliteCommWorker = new SQLiteCommWorker({
            workerId: `sqlite_comm_${process.pid}`,
            writeQueue: this.writeQueue,
            queueName: this.queueName,
            pollInterval: 100,
            batchSize: 50
        });

        // ✅ Durable Queue with serverReady flag
        this.queue = new DurableQueue(this.queueName, {
            visibilityTimeout: 30000,
            maxRetries: 5,
            writeQueue: this.writeQueue,
            commWorker: this.sqliteCommWorker,
            serverReady: this.serverReady  // ✅ Pass serverReady to DurableQueue
        });

        // Stats
        this.stats = {
            jobsProcessed: 0,
            jobsFailed: 0,
            startTime: Date.now()
        };

        this._init();
    }

    // === Private Initialization ===

    _init() {
        this._setupIPCListener();
        this._setupWorkerManager();
        this._setupHeartbeat();

        // ✅ Send ALIVE signal (normal startup, no recover)
        // ✅ If serverReady is true, child will recover and send READY
        if (this.serverReady) {
            // Server was already running when child started (crash restart)
            // Recover already happened in DurableQueue constructor
            this._sendMessage({
                type: 'READY',
                processType: this.processType,
                pid: process.pid,
                timestamp: Date.now()
            });
        } else {
            // Normal startup - no recover, just send ALIVE
            this._sendMessage({
                type: 'ALIVE',
                processType: this.processType,
                pid: process.pid,
                timestamp: Date.now()
            });
        }
    }

    // === Argument Parsing ===

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

    // === IPC Communication ===

    _setupIPCListener() {
        process.on('message', async (message) => {
            if (!message || !message.type) return;

            switch (message.type) {
                case 'NEW_JOB':
                    await this._handleNewJob(message);
                    break;
                case 'GET_STATUS':
                    this._sendStatus();
                    break;
                case 'SHUTDOWN':
                    await this.shutdown();
                    break;
                default:
                    console.log(`[${this.processType}] 📨 Unknown: ${message.type}`);
            }
        });
    }

    async _handleNewJob(message) {
        try {
            const jobData = {
                id: message.jobId,
                data: message.data
            };

            const jobId = await this.queue.enqueue(jobData);
            console.log(`[${this.processType}] 📝 Job ${jobId} enqueued`);
            
            this._sendMessage({
                type: 'JOB_QUEUED',
                jobId,
                timestamp: Date.now()
            });
        } catch (error) {
            console.error(`[${this.processType}] ❌ Failed to enqueue job:`, error.message);
            this._sendMessage({
                type: 'JOB_ERROR',
                jobId: message.jobId,
                error: error.message
            });
        }
    }

    // === Worker Management ===

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
                    
                    this._sendMessage({
                        type: 'JOB_COMPLETE',
                        jobId: job.id,
                        result,
                        timestamp: Date.now()
                    });
                    
                } catch (error) {
                    this.stats.jobsFailed++;
                    console.error(`[${this.processType}] ❌ Worker ${workerId} failed ${job.id}:`, error.message);
                    this._sendMessage({
                        type: 'JOB_FAILED',
                        jobId: job.id,
                        error: error.message,
                        timestamp: Date.now()
                    });
                }
                
            } catch (error) {
                console.error(`[${this.processType}] Worker ${workerId} loop error:`, error);
                await this._sleep(1000);
            }
        }
    }

    // === Job Processing (Override in Child) ===

    async _processJob(job) {
        await this._sleep(1000);
        return {
            jobId: job.id,
            processedAt: new Date().toISOString(),
            data: job.data,
            result: `Processed by ${this.processType}`
        };
    }

    // === Heartbeat ===

    _setupHeartbeat() {
        setInterval(() => {
            if (!this.isRunning) return;
            this._sendMessage({
                type: 'HEARTBEAT',
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
            });
        }, 5000);
    }

    // === Message Helpers ===

    _sendMessage(message) {
        if (process.send) {
            try {
                process.send(message);
            } catch (error) {
                console.error(`[${this.processType}] ❌ Failed to send message:`, error.message);
            }
        }
    }

    // === Utility ===

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    // === Shutdown ===

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

        this._sendMessage({ type: 'SHUTDOWN_COMPLETE' });
        setTimeout(() => process.exit(0), 500);
    }
}

module.exports = BaseChildProcess;