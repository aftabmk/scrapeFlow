// sqlite-server/server.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');
const Queue = require('../queue/Queue');
const SQLQueries = require('./sql-queries');
const SQLiteWriteWorker = require('./workers/write-worker');
const SQLiteReadWorker = require('./workers/read-worker');

class SQLiteServer extends EventEmitter {
    constructor(options = {}) {
        super();
        this.dbPath = options.dbPath || './data/queue.db';
        this.writeWorkers = options.writeWorkers || 1;
        this.readWorkers = options.readWorkers || 3;
        this.queueNames = options.queueNames || ['analyzer', 'browser', 'exporter', 'job-submitter'];
        this.tables = new Map();
        this.isRunning = true;
        this.tablesCreated = false;
        this.db = null;
        this.queries = null;

        this.writeQueue = new Queue({ name: 'write_queue', maxSize: 10000 });
        this.readQueue = new Queue({ name: 'read_queue', maxSize: 10000 });

        this.writeWorker = null;
        this.readWorkersList = [];

        console.log('[SQLiteServer] Constructor complete');
    }

    async start() {
        console.log('[SQLiteServer] Starting server...');

        try {
            this._initDB();
            this._setupIPCListener();
            this._initWorkers();

            // ✅ Create tables and send ALL_TABLES_CREATED
            await this._createAllTables();

            // ✅ Send SQLITE_READY after tables are created
            if (process.send) {
                process.send({
                    type: 'SQLITE_READY',
                    writeWorkers: this.writeWorkers,
                    readWorkers: this.readWorkers,
                    dbPath: this.dbPath,
                    pid: process.pid
                });
            }

            console.log('[SQLiteServer] ✅ Server started successfully');
            return this;
        } catch (error) {
            console.error('[SQLiteServer] ❌ Server start failed:', error.message);
            console.error('[SQLiteServer] Stack:', error.stack);
            throw error;
        }
    }

    _initDB() {
        console.log('[SQLiteServer] Initializing database...');
        console.log(`[SQLiteServer] DB Path: ${this.dbPath}`);

        try {
            const dir = path.dirname(this.dbPath);
            if (!fs.existsSync(dir)) {
                console.log(`[SQLiteServer] Creating directory: ${dir}`);
                fs.mkdirSync(dir, { recursive: true });
            }

            this.db = new DatabaseSync(this.dbPath);
            console.log('[SQLiteServer] ✅ Database connection established');
        } catch (error) {
            console.error('[SQLiteServer] ❌ Failed to connect to database:', error.message);
            throw error;
        }

        try {
            this.db.exec('PRAGMA journal_mode=WAL');
            this.db.exec('PRAGMA synchronous=NORMAL');
            this.db.exec('PRAGMA busy_timeout=5000');

            this.queries = new SQLQueries(this.db);

            this.db.exec(`
                CREATE TABLE IF NOT EXISTS queue_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_name TEXT NOT NULL,
                    op TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    payload TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            `);

            this.db.exec(`
                CREATE TABLE IF NOT EXISTS dead_letter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue_name TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    payload TEXT,
                    retries INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            `);

            this.db.exec(`
                CREATE TABLE IF NOT EXISTS queue_state (
                    queue_name TEXT PRIMARY KEY,
                    pending_count INTEGER DEFAULT 0,
                    in_flight_count INTEGER DEFAULT 0,
                    dead_letter_count INTEGER DEFAULT 0,
                    last_updated TEXT DEFAULT (datetime('now'))
                )
            `);

            console.log('[SQLiteServer] ✅ Database initialized');
        } catch (error) {
            console.error('[SQLiteServer] ❌ Failed to initialize tables:', error.message);
            throw error;
        }
    }

    _createAllTables() {
        return new Promise((resolve, reject) => {
            console.log(`[SQLiteServer] 📋 Creating tables for: ${this.queueNames.join(', ')}`);

            try {
                for (const name of this.queueNames) {
                    const sanitizedName = name.replace(/[^a-zA-Z0-9_]/g, '_');
                    const tableName = `${sanitizedName}_queue`;

                    this.db.exec(`
                    CREATE TABLE IF NOT EXISTS ${tableName} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        job_id TEXT UNIQUE NOT NULL,
                        payload TEXT NOT NULL,
                        status TEXT DEFAULT 'PENDING',
                        retries INTEGER DEFAULT 0,
                        created_at TEXT DEFAULT (datetime('now')),
                        updated_at TEXT DEFAULT (datetime('now'))
                    )
                `);

                    this.db.exec(`
                    CREATE INDEX IF NOT EXISTS idx_${tableName}_status 
                    ON ${tableName}(status, created_at)
                `);

                    this.tables.set(name, tableName);
                    console.log(`[SQLiteServer] ✅ Created table: ${tableName}`);
                }

                this.tablesCreated = true;
                console.log('[SQLiteServer] ✅ ALL tables created!');

                // ✅ CRITICAL FIX: Send signal to parent
                if (process.send) {
                    process.send({ type: 'ALL_TABLES_CREATED', queues: this.queueNames });
                    console.log('[SQLiteServer] 📨 ALL_TABLES_CREATED signal sent to parent');
                } else {
                    console.warn('[SQLiteServer] ⚠️ process.send not available');
                }

                resolve();
            } catch (error) {
                console.error('[SQLiteServer] ❌ Failed to create tables:', error.message);
                reject(error);
            }
        });
    }

    _setupIPCListener() {
        console.log('[SQLiteServer] Setting up IPC listener...');

        process.on('message', async (message) => {
            if (!message) return;

            try {
                const result = await this._handleRequest(message);
                this._sendResponse(message.jobId, result, message.sourcePid);
            } catch (error) {
                console.error('[SQLiteServer] Error handling request:', error.message);
                this._sendResponse(message.jobId, { error: error.message }, message.sourcePid);
            }
        });

        console.log('[SQLiteServer] ✅ IPC listener ready');
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

        const isWrite = ['append', 'deliver', 'ack', 'requeue', 'deadletter'].includes(op);
        const isRead = ['dequeue', 'dequeue_multiple', 'recover', 'stats'].includes(op);

        if (isWrite) {
            this.writeQueue.enqueue({
                op,
                queue,
                jobId,
                payload,
                sourcePid,
                requestId: request.jobId || jobId
            });
            return { queued: true };
        } else if (isRead) {
            this.readQueue.enqueue({
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

        try {
            this.writeWorker = new SQLiteWriteWorker({
                workerId: 'write_worker_1',
                writeQueue: this.writeQueue,
                queries: this.queries,
                sendResponse: (jobId, data, targetPid) => {
                    this._sendResponse(jobId, data, targetPid);
                }
            });

            for (let i = 0; i < this.readWorkers; i++) {
                const worker = new SQLiteReadWorker({
                    workerId: `read_worker_${i + 1}`,
                    readQueue: this.readQueue,
                    queries: this.queries,
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
        } catch (error) {
            console.error('[SQLiteServer] ❌ Failed to initialize workers:', error.message);
            throw error;
        }
    }

    async shutdown() {
        console.log('[SQLiteServer] Shutting down...');
        this.isRunning = false;

        if (this.writeWorker) {
            try {
                this.writeWorker.shutdown();
            } catch (err) { }
        }

        for (const worker of this.readWorkersList) {
            try {
                worker.shutdown();
            } catch (err) { }
        }

        if (this.db) {
            try {
                this.db.close();
            } catch (err) { }
        }

        console.log('[SQLiteServer] ✅ Shutdown complete');
        process.exit(0);
    }
}

module.exports = SQLiteServer;