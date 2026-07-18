// sqlite/worker.js
const { parentPort, workerData } = require('worker_threads');
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');

class SQLiteWorker {
    constructor() {
        this.id = workerData.id || `sqlite_${Date.now()}`;
        this.type = 'sqlite';
        this.isRunning = true;
        
        this.dbPath = workerData.dbPath || './data/queue.db';
        this.readWorkers = workerData.readWorkers || 2;
        this.writeWorkers = workerData.writeWorkers || 2;
        this.batchSize = workerData.batchSize || 50;
        this.cacheSize = workerData.cacheSize || 2000;
        
        this.db = null;
        this.isReady = false;
        this.pendingWrites = [];
        this.cache = new Map();
        
        this.sendReady();
        this.initDatabase();
        this.start();
    }

    sendReady() {
        if (parentPort) {
            parentPort.postMessage({
                type: 'worker.ready',
                workerId: this.id,
                workerType: this.type,
                timestamp: Date.now()
            });
        }
    }

    initDatabase() {
        try {
            const dir = path.dirname(this.dbPath);
            if (!fs.existsSync(dir)) {
                fs.mkdirSync(dir, { recursive: true });
            }
            
            this.db = new DatabaseSync(this.dbPath);
            this.db.exec('PRAGMA journal_mode=WAL');
            this.db.exec('PRAGMA synchronous=NORMAL');
            this.db.exec(`PRAGMA cache_size=${this.cacheSize}`);
            this.db.exec('PRAGMA busy_timeout=5000');
            this.db.exec('PRAGMA foreign_keys=ON');
            
            this.createTables();
            
            this.isReady = true;
            
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SQLITE_READY',
                    workerId: this.id,
                    timestamp: Date.now()
                });
            }
            
            console.log(`[SQLiteWorker ${this.id}] ✅ Ready`);
            
        } catch (error) {
            console.error(`[SQLiteWorker ${this.id}] ❌ Init error:`, error.message);
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SQLITE_ERROR',
                    error: error.message,
                    timestamp: Date.now()
                });
            }
        }
    }

    createTables() {
        try {
            // Main jobs table
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    status TEXT NOT NULL,
                    data TEXT NOT NULL,
                    metadata TEXT,
                    attempts INTEGER DEFAULT 0,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    error TEXT
                )
            `);
            
            // Queue log - tracks all operations
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS queue_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    data TEXT,
                    timestamp INTEGER NOT NULL
                )
            `);
            
            // In-flight jobs tracking
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS in_flight (
                    job_id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    worker_id TEXT,
                    started_at INTEGER NOT NULL,
                    expires_at INTEGER NOT NULL
                )
            `);
            
            // Checkpoints for full state recovery
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            `);
            
            // Dead letter queue
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS dead_letter (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    queue TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    job_data TEXT NOT NULL,
                    error TEXT,
                    attempts INTEGER DEFAULT 0,
                    timestamp INTEGER NOT NULL
                )
            `);
            
            // Completed jobs archive (for history)
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS completed_jobs (
                    id TEXT PRIMARY KEY,
                    queue TEXT NOT NULL,
                    data TEXT NOT NULL,
                    metadata TEXT,
                    completed_at INTEGER NOT NULL
                )
            `);
            
            // Indexes for performance
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue, status)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_queue ON queue_log(queue, timestamp)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_job ON queue_log(job_id)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_in_flight_queue ON in_flight(queue)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_in_flight_expires ON in_flight(expires_at)`);
            
            console.log(`[SQLiteWorker ${this.id}] ✅ Tables created`);
            
        } catch (error) {
            console.error(`[SQLiteWorker ${this.id}] ❌ Table creation error:`, error.message);
            throw error;
        }
    }

    start() {
        if (parentPort) {
            parentPort.on('message', async (message) => {
                await this.handleMessage(message);
            });
        }
    }

    async handleMessage(message) {
        if (!message || !message.type) return;
        
        switch (message.type) {
            case 'SQLITE_REQUEST':
                await this.handleRequest(message);
                break;
                
            case 'SHUTDOWN':
                this.shutdown();
                break;
                
            default:
                console.log(`[SQLiteWorker ${this.id}] Unknown: ${message.type}`);
        }
    }

    async handleRequest(message) {
        const { requestId, operation, queue, jobId, data } = message.payload || {};
        
        try {
            let result;
            
            switch (operation) {
                // Write operations
                case 'append':
                    result = this.append(queue, jobId, data);
                    break;
                    
                case 'deliver':
                    result = this.deliver(queue, jobId);
                    break;
                    
                case 'ack':
                    result = this.ack(queue, jobId);
                    break;
                    
                case 'requeue':
                    result = this.requeue(queue, jobId);
                    break;
                    
                case 'deadletter':
                    result = this.deadletter(queue, jobId, data);
                    break;
                    
                case 'complete':
                    result = this.complete(queue, jobId, data);
                    break;
                    
                // Read operations
                case 'dequeue':
                    result = this.dequeue(queue, jobId);
                    break;
                    
                case 'dequeue_multiple':
                    result = this.dequeueMultiple(queue, jobId, data?.count || 1);
                    break;
                    
                case 'recover':
                    result = this.recover(queue);
                    break;
                    
                case 'recover_all':
                    result = this.recoverAll();
                    break;
                    
                case 'stats':
                    result = this.stats(queue);
                    break;
                    
                case 'get_in_flight':
                    result = this.getInFlight(queue);
                    break;
                    
                case 'get_pending':
                    result = this.getPending(queue);
                    break;
                    
                case 'batch_write':
                    result = this.batchWrite(data);
                    break;
                    
                case 'save_checkpoint':
                    result = this.saveCheckpoint(data);
                    break;
                    
                case 'load_checkpoint':
                    result = this.loadCheckpoint(data?.id);
                    break;
                    
                default:
                    throw new Error(`Unknown operation: ${operation}`);
            }
            
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SQLITE_RESPONSE',
                    requestId,
                    payload: result,
                    timestamp: Date.now()
                });
            }
            
        } catch (error) {
            console.error(`[SQLiteWorker ${this.id}] ❌ ${operation} failed:`, error.message);
            
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SQLITE_RESPONSE',
                    requestId,
                    payload: { error: error.message },
                    timestamp: Date.now()
                });
            }
        }
    }

    // === Write Operations ===

    append(queue, jobId, payload) {
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at)
            VALUES (?, ?, 'PENDING', ?, ?, 0, ?, ?)
        `);
        stmt.run(jobId, queue, JSON.stringify(payload), JSON.stringify({}), Date.now(), Date.now());
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, data, timestamp)
            VALUES ('${queue}', '${jobId}', 'append', '${JSON.stringify(payload)}', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'pending' };
    }

    deliver(queue, jobId) {
        const stmt = this.db.prepare(`
            UPDATE jobs SET status = 'IN_FLIGHT', updated_at = ?
            WHERE job_id = ? AND status = 'PENDING'
        `);
        const result = stmt.run(Date.now(), jobId);
        
        if (result.changes === 0) {
            // Check if job exists in different state
            const checkStmt = this.db.prepare(`SELECT status FROM jobs WHERE job_id = ?`);
            const row = checkStmt.get(jobId);
            if (row) {
                throw new Error(`Job ${jobId} already in status: ${row.status}`);
            }
            throw new Error(`Job ${jobId} not found`);
        }
        
        // Add to in-flight tracking
        const inFlightStmt = this.db.prepare(`
            INSERT OR REPLACE INTO in_flight (job_id, queue, started_at, expires_at)
            VALUES (?, ?, ?, ?)
        `);
        inFlightStmt.run(jobId, queue, Date.now(), Date.now() + 30000);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'deliver', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'in_flight' };
    }

    ack(queue, jobId) {
        const stmt = this.db.prepare(`DELETE FROM jobs WHERE job_id = ?`);
        const result = stmt.run(jobId);
        
        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found for ACK`);
        }
        
        // Remove from in-flight
        const inFlightStmt = this.db.prepare(`DELETE FROM in_flight WHERE job_id = ?`);
        inFlightStmt.run(jobId);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'ack', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'completed' };
    }

    complete(queue, jobId, data) {
        // Move to completed jobs archive
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO completed_jobs (id, queue, data, metadata, completed_at)
            VALUES (?, ?, ?, ?, ?)
        `);
        stmt.run(jobId, queue, JSON.stringify(data), JSON.stringify({}), Date.now());
        
        // Remove from main jobs
        const deleteStmt = this.db.prepare(`DELETE FROM jobs WHERE job_id = ?`);
        deleteStmt.run(jobId);
        
        // Remove from in-flight
        const inFlightStmt = this.db.prepare(`DELETE FROM in_flight WHERE job_id = ?`);
        inFlightStmt.run(jobId);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'complete', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'completed' };
    }

    requeue(queue, jobId) {
        const stmt = this.db.prepare(`
            UPDATE jobs SET status = 'PENDING', attempts = attempts + 1, updated_at = ?
            WHERE job_id = ?
        `);
        const result = stmt.run(Date.now(), jobId);
        
        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found for requeue`);
        }
        
        // Remove from in-flight
        const inFlightStmt = this.db.prepare(`DELETE FROM in_flight WHERE job_id = ?`);
        inFlightStmt.run(jobId);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'requeue', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'pending' };
    }

    deadletter(queue, jobId, payload) {
        const stmt = this.db.prepare(`
            INSERT INTO dead_letter (queue, job_id, job_data, attempts, timestamp)
            VALUES (?, ?, ?, ?, ?)
        `);
        stmt.run(queue, jobId, JSON.stringify(payload), payload?.retries || 0, Date.now());
        
        const deleteStmt = this.db.prepare(`DELETE FROM jobs WHERE job_id = ?`);
        deleteStmt.run(jobId);
        
        const inFlightStmt = this.db.prepare(`DELETE FROM in_flight WHERE job_id = ?`);
        inFlightStmt.run(jobId);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'deadletter', ${Date.now()})
        `);
        
        return { success: true, jobId, status: 'deadletter' };
    }

    // === Read Operations ===

    dequeue(queue, workerId) {
        const stmt = this.db.prepare(`
            SELECT id, data FROM jobs
            WHERE queue = ? AND status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT 1
        `);
        const row = stmt.get(queue);
        
        if (!row) {
            return { job: null };
        }
        
        // Update to in-flight
        const updateStmt = this.db.prepare(`
            UPDATE jobs SET status = 'IN_FLIGHT', updated_at = ?
            WHERE id = ?
        `);
        updateStmt.run(Date.now(), row.id);
        
        // Add to in-flight tracking
        const inFlightStmt = this.db.prepare(`
            INSERT OR REPLACE INTO in_flight (job_id, queue, worker_id, started_at, expires_at)
            VALUES (?, ?, ?, ?, ?)
        `);
        inFlightStmt.run(row.id, queue, workerId, Date.now(), Date.now() + 30000);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${row.id}', 'dequeue', ${Date.now()})
        `);
        
        return {
            job: {
                job_id: row.id,
                payload: JSON.parse(row.data)
            }
        };
    }

    dequeueMultiple(queue, workerId, count) {
        const maxCount = Math.min(count || 1, 10);
        const jobs = [];
        
        for (let i = 0; i < maxCount; i++) {
            const result = this.dequeue(queue, workerId);
            if (result.job) {
                jobs.push(result.job);
            } else {
                break;
            }
        }
        
        return { jobs };
    }

    recover(queue) {
        // Recover pending jobs
        const pendingStmt = this.db.prepare(`
            SELECT id, data FROM jobs
            WHERE queue = ? AND status = 'PENDING'
            ORDER BY created_at ASC
        `);
        const pendingRows = pendingStmt.all(queue);
        
        // Recover in-flight jobs (worker crashed)
        const inFlightStmt = this.db.prepare(`
            SELECT job_id, data FROM jobs j
            JOIN in_flight i ON j.id = i.job_id
            WHERE j.queue = ? AND j.status = 'IN_FLIGHT'
        `);
        const inFlightRows = inFlightStmt.all(queue);
        
        return {
            rows: [
                ...pendingRows.map(row => ({
                    id: row.id,
                    payload: JSON.parse(row.data),
                    op: 'append'
                })),
                ...inFlightRows.map(row => ({
                    id: row.job_id,
                    payload: JSON.parse(row.data),
                    op: 'requeue'  // Requeue in-flight jobs
                }))
            ]
        };
    }

    recoverAll() {
        const result = {};
        
        // Get all queues
        const queueStmt = this.db.prepare(`SELECT DISTINCT queue FROM jobs`);
        const queues = queueStmt.all();
        
        for (const { queue } of queues) {
            result[queue] = this.recover(queue);
        }
        
        return result;
    }

    getInFlight(queue) {
        const stmt = this.db.prepare(`
            SELECT j.id, j.data, i.started_at, i.expires_at, i.worker_id
            FROM jobs j
            JOIN in_flight i ON j.id = i.job_id
            WHERE j.queue = ? AND j.status = 'IN_FLIGHT'
        `);
        const rows = stmt.all(queue);
        
        return rows.map(row => ({
            job_id: row.id,
            payload: JSON.parse(row.data),
            started_at: row.started_at,
            expires_at: row.expires_at,
            worker_id: row.worker_id
        }));
    }

    getPending(queue) {
        const stmt = this.db.prepare(`
            SELECT id, data FROM jobs
            WHERE queue = ? AND status = 'PENDING'
            ORDER BY created_at ASC
        `);
        const rows = stmt.all(queue);
        
        return rows.map(row => ({
            job_id: row.id,
            payload: JSON.parse(row.data)
        }));
    }

    stats(queue) {
        const stmt = this.db.prepare(`
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'IN_FLIGHT' THEN 1 ELSE 0 END) as in_flight,
                SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) as complete,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
            FROM jobs WHERE queue = ?
        `);
        const row = stmt.get(queue);
        
        // Get dead letter count
        const deadStmt = this.db.prepare(`SELECT COUNT(*) as count FROM dead_letter WHERE queue = ?`);
        const deadRow = deadStmt.get(queue);
        
        // Get in-flight count
        const inFlightCountStmt = this.db.prepare(`SELECT COUNT(*) as count FROM in_flight WHERE queue = ?`);
        const inFlightCountRow = inFlightCountStmt.get(queue);
        
        return {
            total: row.total || 0,
            pending: row.pending || 0,
            in_flight: row.in_flight || 0,
            in_flight_tracked: inFlightCountRow?.count || 0,
            complete: row.complete || 0,
            failed: row.failed || 0,
            dead_letter: deadRow?.count || 0,
            queue
        };
    }

    batchWrite(entries) {
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at, completed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        this.db.exec('BEGIN TRANSACTION');
        
        try {
            for (const entry of entries) {
                stmt.run(
                    entry.id,
                    entry.queue,
                    entry.status,
                    JSON.stringify(entry.data),
                    JSON.stringify(entry.metadata || {}),
                    entry.attempts || 0,
                    entry.created_at || Date.now(),
                    Date.now(),
                    entry.completed_at || null,
                    entry.error || null
                );
                
                // Log operation
                if (entry.operation) {
                    const logStmt = this.db.prepare(`
                        INSERT INTO queue_log (queue, job_id, operation, data, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                    `);
                    logStmt.run(entry.queue, entry.id, entry.operation, JSON.stringify(entry.data), Date.now());
                }
            }
            
            this.db.exec('COMMIT');
            return { success: true, count: entries.length };
        } catch (error) {
            this.db.exec('ROLLBACK');
            throw error;
        }
    }

    saveCheckpoint(checkpoint) {
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO checkpoints (id, state, timestamp)
            VALUES (?, ?, ?)
        `);
        stmt.run(checkpoint.id, JSON.stringify(checkpoint.state), checkpoint.timestamp);
        return { success: true, id: checkpoint.id };
    }

    loadCheckpoint(id = null) {
        let stmt;
        if (id) {
            stmt = this.db.prepare(`SELECT * FROM checkpoints WHERE id = ?`);
            const row = stmt.get(id);
            if (row) {
                return { id: row.id, state: JSON.parse(row.state), timestamp: row.timestamp };
            }
            return null;
        }
        
        stmt = this.db.prepare(`SELECT * FROM checkpoints ORDER BY timestamp DESC LIMIT 1`);
        const row = stmt.get();
        if (row) {
            return { id: row.id, state: JSON.parse(row.state), timestamp: row.timestamp };
        }
        return null;
    }

    shutdown() {
        console.log(`[SQLiteWorker ${this.id}] Shutting down...`);
        this.isRunning = false;
        
        // Final checkpoint
        const checkpoint = {
            id: `ckpt_${Date.now()}`,
            state: { 
                status: 'shutdown',
                timestamp: Date.now()
            },
            timestamp: Date.now()
        };
        this.saveCheckpoint(checkpoint);
        
        if (this.db) {
            try {
                this.db.close();
            } catch (err) {}
        }
        
        this.cache.clear();
        
        if (parentPort) {
            parentPort.postMessage({
                type: 'worker.shutdown',
                workerId: this.id,
                timestamp: Date.now()
            });
        }
    }
}

if (require.main === module) new SQLiteWorker();
module.exports = SQLiteWorker;