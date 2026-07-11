// sqlite-server/server.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');
const NormalQueue = require('../queue/normal-queue');
const SQLQueries = require('./sql-queries');
const SQLiteWriteWorker = require('./workers/write-worker');
const SQLiteReadWorker = require('./workers/read-worker');

class SQLiteServer extends EventEmitter {
    constructor(options = {}) {
        super();
        this.dbPath = options.dbPath || './data/queue.db';
        this.writeWorkers = options.writeWorkers || 1;
        this.readWorkers = options.readWorkers || 3;
        this.tables = new Map();
        this.isRunning = true;

        // ✅ Normal queues for request routing
        this.writeQueue = new NormalQueue({ name: 'write_queue', maxSize: 10000 });
        this.readQueue = new NormalQueue({ name: 'read_queue', maxSize: 10000 });

        // ✅ SQL Queries instance
        this.queries = null;

        // ✅ Workers
        this.writeWorker = null;
        this.readWorkersList = [];

        this._initDB();
        this._setupIPCListener();
        this._initWorkers();
    }

    _initDB() {
        const dir = path.dirname(this.dbPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        this.db = new DatabaseSync(this.dbPath);

        this.db.exec('PRAGMA journal_mode=WAL');
        this.db.exec('PRAGMA synchronous=NORMAL');
        this.db.exec('PRAGMA busy_timeout=5000');

        // ✅ Create SQL Queries instance
        this.queries = new SQLQueries(this.db);

        // ✅ Create queue log table
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS queue_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                op TEXT NOT NULL,
                job_id TEXT NOT NULL,
                payload TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // ✅ Dead letter table
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS dead_letter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                payload TEXT,
                retries INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // ✅ Queue state tracking
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS queue_state (
                queue_name TEXT PRIMARY KEY,
                pending_count INTEGER DEFAULT 0,
                in_flight_count INTEGER DEFAULT 0,
                dead_letter_count INTEGER DEFAULT 0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        console.log('[SQLiteServer] ✅ Database initialized');
    }

    _setupIPCListener() {
        process.on('message', async (message) => {
            if (!message) return;

            try {
                const result = await this._handleRequest(message);
                this._sendResponse(message.jobId, result, message.sourcePid);
            } catch (error) {
                this._sendResponse(message.jobId, { error: error.message }, message.sourcePid);
            }
        });
    }

    _sendResponse(jobId, data, targetPid) {
        if (process.send) {
            process.send({
                type: 'SQLITE_RESPONSE',
                targetPid: targetPid || process.ppid,
                jobId,
                ...data
            });
        }
    }

    async _handleRequest(request) {
        const { op, queue, jobId, payload, sourcePid } = request;

        // ✅ Ensure queue table exists
        this.queries.getOrCreateTable(queue);

        const isWrite = ['append', 'deliver', 'ack', 'requeue', 'deadletter'].includes(op);
        const isRead = ['dequeue', 'dequeue_multiple', 'recover', 'stats'].includes(op);

        if (isWrite) {
            await this.writeQueue.enqueue({
                op,
                queue,
                jobId,
                payload,
                sourcePid,
                requestId: request.jobId || jobId
            });
            return { queued: true };
        } else if (isRead) {
            await this.readQueue.enqueue({
                op,
                queue,
                jobId,
                payload,
                sourcePid,
                requestId: request.jobId || jobId
            });
            return { queued: true };
        } else {
            throw new Error(`Unknown operation: ${op}`);
        }
    }

    _initWorkers() {
        console.log('[SQLiteServer] Initializing workers...');

        // ✅ Create Write Worker (Single)
        this.writeWorker = new SQLiteWriteWorker({
            workerId: 'write_worker_1',
            writeQueue: this.writeQueue,
            queries: this.queries,  // ✅ Pass SQL queries
            sendResponse: (jobId, data, targetPid) => {
                this._sendResponse(jobId, data, targetPid);
            }
        });

        // ✅ Create Read Workers (Multiple)
        for (let i = 0; i < this.readWorkers; i++) {
            const worker = new SQLiteReadWorker({
                workerId: `read_worker_${i + 1}`,
                readQueue: this.readQueue,
                queries: this.queries,  // ✅ Pass SQL queries
                sendResponse: (jobId, data, targetPid) => {
                    this._sendResponse(jobId, data, targetPid);
                }
            });
            
            this.readWorkersList.push(worker);
        }

        console.log(`[SQLiteServer] ✅ Workers initialized: 1 write + ${this.readWorkers} read`);
        
        this.emit('ready', {
            writeWorkers: this.writeWorkers,
            readWorkers: this.readWorkers,
            dbPath: this.dbPath,
            pid: process.pid
        });
    }

    async shutdown() {
        console.log('[SQLiteServer] Shutting down...');
        this.isRunning = false;

        if (this.writeWorker) {
            this.writeWorker.shutdown();
        }

        for (const worker of this.readWorkersList) {
            worker.shutdown();
        }

        if (this.db) {
            this.db.close();
        }

        console.log('[SQLiteServer] ✅ Shutdown complete');
        process.exit(0);
    }
}

module.exports = SQLiteServer;