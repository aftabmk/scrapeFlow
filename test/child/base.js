// child/base.js
const { EventEmitter } = require('events');
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
        this.isRunning = true;

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
            commWorker: this.sqliteCommWorker
        });

        this.stats = {
            jobsProcessed: 0,
            jobsFailed: 0,
            startTime: Date.now()
        };

        this._init();
    }

    _init() {
        console.log(`[${this.processType}] 🟢 Starting with ${this.processingWorkers} workers`);
        console.log(`[${this.processType}] 📋 Queue: ${this.queueName}`);

        this._setupIPCListener();
        this._setupWorkerManager();
        this._setupHeartbeat();
        this._sendReady();
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

            this._sendMessage({
                type: 'JOB_QUEUED',
                jobId,
                timestamp: Date.now()
            });
            
            console.log(`[${this.processType}] 📝 Job ${jobId} enqueued`);
        } catch (error) {
            console.error(`[${this.processType}] ❌ Failed to enqueue job:`, error.message);
            this._sendMessage({
                type: 'JOB_ERROR',
                jobId: message.jobId,
                error: error.message
            });
        }
    }

    _setupWorkerManager() {
        if (this.processingWorkers === 0) {
            console.log(`[${this.processType}] ⏳ No workers (waiting for messages)`);
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
                    // ✅ Log every 50 empty checks (25 seconds at 500ms)
                    if (emptyCount % 50 === 0) {
                        console.log(`[${this.processType}] ⏳ Worker ${workerId} waiting for jobs...`);
                    }
                    await this._sleep(500);
                    continue;
                }
                
                // ✅ Reset empty counter
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
                    
                    // ✅ Let the sweeper handle requeue
                }
                
            } catch (error) {
                console.error(`[${this.processType}] Worker ${workerId} loop error:`, error);
                await this._sleep(1000);
            }
        }
        
        console.log(`[${this.processType}] 👷 Worker ${workerId} stopped`);
    }

    async _processJob(job) {
        // Override in child classes
        await this._sleep(1000);
        return {
            jobId: job.id,
            processedAt: new Date().toISOString(),
            data: job.data,
            result: `Processed by ${this.processType}`
        };
    }

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

    _sendMessage(message) {
        if (process.send) {
            try {
                process.send(message);
            } catch (error) {
                console.error(`[${this.processType}] ❌ Failed to send message:`, error.message);
            }
        }
    }

    _sendReady() {
        this._sendMessage({
            type: 'ready',
            processType: this.processType,
            processingWorkers: this.processingWorkers,
            queueName: this.queueName,
            pid: process.pid
        });
    }

    _sendStatus() {
        this._sendMessage({
            type: 'STATUS',
            processType: this.processType,
            activeJobs: this.queue.getInFlightCount(),
            queueSize: this.queue.queue.getSize(),
            writeQueueSize: this.queue.getWriteQueueSize(),
            jobsProcessed: this.stats.jobsProcessed,
            jobsFailed: this.stats.jobsFailed,
            pid: process.pid
        });
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

        this._sendMessage({ type: 'SHUTDOWN_COMPLETE' });
        setTimeout(() => process.exit(0), 500);
    }
}

module.exports = BaseChildProcess;