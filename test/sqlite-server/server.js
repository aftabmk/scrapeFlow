// sqlite-server/server.js
const { EventEmitter } = require('events');
const SQLiteManager = require('./components/sqlite-manager');
const RequestRouter = require('./components/request-router');
const ResponseSender = require('./components/response-sender');
const IPCHandler = require('./components/ipc-handler');
const SQLiteWriteWorker = require('./workers/write-worker');
const SQLiteReadWorker = require('./workers/read-worker');

class SQLiteServer extends EventEmitter {
    constructor(options = {}) {
        super();
        this.dbPath = options.dbPath || './data/queue.db';
        this.writeWorkers = options.writeWorkers || 1;
        this.readWorkers = options.readWorkers || 3;
        this.isRunning = true;

        // ✅ Initialize components
        this.sqliteManager = new SQLiteManager({ dbPath: this.dbPath });
        this.requestRouter = new RequestRouter();
        this.responseSender = new ResponseSender({
            sendFn: (jobId, data, targetPid) => this._sendResponse(jobId, data, targetPid)
        });
        this.ipcHandler = new IPCHandler({
            requestRouter: this.requestRouter,
            responseSender: this.responseSender,
            sqliteManager: this.sqliteManager
        });

        // ✅ Workers
        this.writeWorker = null;
        this.readWorkersList = [];

        console.log('[SQLiteServer] Constructor complete');
    }

    /**
     * Start the server
     */
    async start() {
        console.log('[SQLiteServer] Starting server...');

        try {
            // 1. Initialize database
            this.sqliteManager.initialize();

            // 2. Setup IPC handler
            this.ipcHandler.start();

            // 3. Initialize workers
            this._initWorkers();

            // 4. Signal ready
            this.emit('ready', {
                writeWorkers: this.writeWorkers,
                readWorkers: this.readWorkers,
                dbPath: this.dbPath,
                pid: process.pid
            });

            console.log('[SQLiteServer] ✅ Server started successfully');
            return this;

        } catch (error) {
            console.error('[SQLiteServer] ❌ Server start failed:', error);
            throw error;
        }
    }

    /**
     * Initialize workers
     */
    _initWorkers() {
        console.log('[SQLiteServer] Initializing workers...');

        const queries = this.sqliteManager.getQueries();
        const db = this.sqliteManager.getDB();

        // Create Write Worker (Single)
        this.writeWorker = new SQLiteWriteWorker({
            workerId: 'write_worker_1',
            writeQueue: this.requestRouter.getWriteQueue(),
            queries: queries,
            sendResponse: (jobId, data, targetPid) => {
                this.responseSender.send(jobId, data, targetPid);
            }
        });

        // Create Read Workers (Multiple)
        for (let i = 0; i < this.readWorkers; i++) {
            const worker = new SQLiteReadWorker({
                workerId: `read_worker_${i + 1}`,
                readQueue: this.requestRouter.getReadQueue(),
                queries: queries,
                sendResponse: (jobId, data, targetPid) => {
                    this.responseSender.send(jobId, data, targetPid);
                }
            });

            this.readWorkersList.push(worker);
        }

        console.log(`[SQLiteServer] ✅ Workers initialized: 1 write + ${this.readWorkers} read`);
    }

    /**
     * Send response via IPC
     */
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

    /**
     * Shutdown
     */
    async shutdown() {
        console.log('[SQLiteServer] Shutting down...');
        this.isRunning = false;

        // Shutdown workers
        if (this.writeWorker) {
            this.writeWorker.shutdown();
        }

        for (const worker of this.readWorkersList) {
            worker.shutdown();
        }

        // Shutdown components
        this.ipcHandler.shutdown();
        this.requestRouter.shutdown();
        this.responseSender.shutdown();
        this.sqliteManager.close();

        console.log('[SQLiteServer] ✅ Shutdown complete');
        process.exit(0);
    }
}

module.exports = SQLiteServer;