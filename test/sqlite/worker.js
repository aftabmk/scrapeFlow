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
            
            this.db.exec(`
                CREATE TABLE IF NOT EXISTS checkpoints (
                    id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    timestamp INTEGER NOT NULL
                )
            `);
            
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
            
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue, status)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_queue ON queue_log(queue, timestamp)`);
            this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_job ON queue_log(job_id)`);
            
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
                    
                case 'dequeue':
                    result = this.dequeue(queue, jobId);
                    break;
                    
                case 'dequeue_multiple':
                    result = this.dequeueMultiple(queue, jobId, data?.count || 1);
                    break;
                    
                case 'recover':
                    result = this.recover(queue);
                    break;
                    
                case 'stats':
                    result = this.stats(queue);
                    break;
                    
                case 'batch_write':
                    result = this.batchWrite(data);
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

    // Database operations
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
        
        return { success: true, jobId };
    }

    deliver(queue, jobId) {
        const stmt = this.db.prepare(`
            UPDATE jobs SET status = 'IN_FLIGHT', updated_at = ?
            WHERE job_id = ? AND status = 'PENDING'
        `);
        const result = stmt.run(Date.now(), jobId);
        
        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found or already delivered`);
        }
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'deliver', ${Date.now()})
        `);
        
        return { success: true, jobId };
    }

    ack(queue, jobId) {
        const stmt = this.db.prepare(`DELETE FROM jobs WHERE job_id = ?`);
        const result = stmt.run(jobId);
        
        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found for ACK`);
        }
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'ack', ${Date.now()})
        `);
        
        return { success: true, jobId };
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
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'requeue', ${Date.now()})
        `);
        
        return { success: true, jobId };
    }

    deadletter(queue, jobId, payload) {
        const stmt = this.db.prepare(`
            INSERT INTO dead_letter (queue, job_id, job_data, attempts, timestamp)
            VALUES (?, ?, ?, ?, ?)
        `);
        stmt.run(queue, jobId, JSON.stringify(payload), payload?.retries || 0, Date.now());
        
        const deleteStmt = this.db.prepare(`DELETE FROM jobs WHERE job_id = ?`);
        deleteStmt.run(jobId);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${jobId}', 'deadletter', ${Date.now()})
        `);
        
        return { success: true, jobId };
    }

    dequeue(queue, workerId) {
        const stmt = this.db.prepare(`
            SELECT job_id, payload FROM jobs
            WHERE status = 'PENDING' AND queue = ?
            ORDER BY created_at ASC
            LIMIT 1
        `);
        const row = stmt.get(queue);
        
        if (!row) {
            return { job: null };
        }
        
        const updateStmt = this.db.prepare(`
            UPDATE jobs SET status = 'IN_FLIGHT', updated_at = ?
            WHERE job_id = ?
        `);
        updateStmt.run(Date.now(), row.job_id);
        
        this.db.exec(`
            INSERT INTO queue_log (queue, job_id, operation, timestamp)
            VALUES ('${queue}', '${row.job_id}', 'deliver', ${Date.now()})
        `);
        
        return {
            job: {
                job_id: row.job_id,
                payload: JSON.parse(row.payload)
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
        const stmt = this.db.prepare(`
            SELECT job_id, payload FROM jobs
            WHERE queue = ? AND status IN ('PENDING', 'IN_FLIGHT')
            ORDER BY created_at ASC
        `);
        const rows = stmt.all(queue);
        
        return {
            rows: rows.map(row => ({
                id: row.job_id,
                payload: JSON.parse(row.payload),
                op: 'append'
            }))
        };
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
        
        return {
            total: row.total || 0,
            pending: row.pending || 0,
            in_flight: row.in_flight || 0,
            complete: row.complete || 0,
            failed: row.failed || 0,
            queue
        };
    }

    batchWrite(entries) {
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at, completed_at, error)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);
        
        this.db.exec('BEGIN TRANSACTION');
        
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
        }
        
        this.db.exec('COMMIT');
        
        return { success: true, count: entries.length };
    }

    shutdown() {
        console.log(`[SQLiteWorker ${this.id}] Shutting down...`);
        this.isRunning = false;
        
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