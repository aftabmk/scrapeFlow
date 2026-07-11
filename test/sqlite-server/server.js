// sqlite-server/server.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');

class SQLiteServer extends EventEmitter {
    constructor(options = {}) {
        super();
        this.dbPath = options.dbPath || './data/queue.db';
        this.tables = new Map();
        this.pendingRequests = new Map();
        this.isRunning = true;

        this._initDB();
        this._setupIPCListener();
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

        this.db.exec(`
            CREATE INDEX IF NOT EXISTS idx_queue_log_queue 
            ON queue_log(queue_name, created_at)
        `);

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

    _getQueueTable(queueName) {
        if (!this.tables.has(queueName)) {
            const tableName = `queue_${queueName.replace(/[^a-zA-Z0-9_]/g, '_')}`;

            this.db.exec(`
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT UNIQUE NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT DEFAULT 'PENDING',
                    retries INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            this.db.exec(`
                CREATE INDEX IF NOT EXISTS idx_${tableName}_status 
                ON ${tableName}(status, created_at)
            `);

            this.tables.set(queueName, tableName);
            console.log(`[SQLiteServer] ✅ Created table: ${tableName}`);
        }

        return this.tables.get(queueName);
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
                jobId,  // ✅ exchange-contract
                ...data
            });
        }
    }

    // sqlite-server/server.js
    async _handleRequest(request) {
        const { op, queue, jobId, payload, sourcePid } = request;

        let result;
        let responseJobId;

        switch (op) {
            case 'append':
                result = this._handleAppend(queue, jobId, payload);
                responseJobId = jobId;
                break;
            case 'deliver':
                result = this._handleDeliver(queue, jobId);
                responseJobId = jobId;
                break;
            case 'ack':
                result = this._handleAck(queue, jobId);
                responseJobId = jobId;
                break;
            case 'recover':
                result = this._handleRecover(queue);
                // ✅ Use the same jobId (queue name) for tracking
                responseJobId = jobId;  // ✅ recover_browser_queue
                console.log(`[SQLiteServer] 🔄 Recover response for: ${responseJobId}`);
                break;
            case 'requeue':
                result = this._handleRequeue(queue, jobId);
                responseJobId = jobId;
                break;
            case 'deadletter':
                result = this._handleDeadLetter(queue, jobId, payload);
                responseJobId = jobId;
                break;
            case 'stats':
                result = this._handleStats(queue);
                responseJobId = jobId || 'stats';
                break;
            default:
                throw new Error(`Unknown operation: ${op}`);
        }

        // ✅ Send response with the same tracking ID
        this._sendResponse(responseJobId, result, sourcePid);
    }

    // === Operation Handlers ===

    _handleAppend(queue, jobId, payload) {
        const table = this._getQueueTable(queue);
        const stmt = this.db.prepare(`
            INSERT OR REPLACE INTO ${table} (job_id, payload, status)
            VALUES (?, ?, 'PENDING')
        `);
        stmt.run(jobId, JSON.stringify(payload));

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id, payload)
            VALUES ('${queue}', 'append', '${jobId}', '${JSON.stringify(payload)}')
        `);

        this._updateQueueState(queue);
        return { success: true };
    }

    _handleDeliver(queue, jobId) {
        const table = this._getQueueTable(queue);

        const stmt = this.db.prepare(`
            UPDATE ${table} 
            SET status = 'IN_FLIGHT', updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ? AND status = 'PENDING'
        `);
        const result = stmt.run(jobId);

        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found or already delivered`);
        }

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'deliver', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true };
    }

    _handleAck(queue, jobId) {
        const table = this._getQueueTable(queue);

        const stmt = this.db.prepare(`
            DELETE FROM ${table} WHERE job_id = ?
        `);
        const result = stmt.run(jobId);

        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found`);
        }

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'ack', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true };
    }

    _handleRecover(queue) {
        const table = this._getQueueTable(queue);

        // ✅ Get ALL un-ACKed jobs (PENDING + IN_FLIGHT)
        const stmt = this.db.prepare(`
            SELECT job_id, payload FROM ${table} 
            WHERE status IN ('PENDING', 'IN_FLIGHT')
            ORDER BY created_at ASC
        `);
        const rows = stmt.all();

        console.log(`[SQLiteServer] 🔄 Recovering ${rows.length} un-ACKed jobs for ${queue}`);

        return {
            rows: rows.map(row => ({
                id: row.job_id,  // ✅ exchange-contract
                payload: JSON.parse(row.payload),
                op: 'append'
            }))
        };
    }

    _handleRequeue(queue, jobId) {
        const table = this._getQueueTable(queue);

        const stmt = this.db.prepare(`
            UPDATE ${table} 
            SET status = 'PENDING', 
                retries = retries + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
        `);
        const result = stmt.run(jobId);

        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found`);
        }

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'requeue', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true };
    }

    _handleDeadLetter(queue, jobId, payload) {
        const stmt = this.db.prepare(`
            INSERT INTO dead_letter (queue_name, job_id, payload, retries)
            VALUES (?, ?, ?, ?)
        `);
        stmt.run(queue, jobId, JSON.stringify(payload), payload?.retries || 0);

        const table = this._getQueueTable(queue);
        const deleteStmt = this.db.prepare(`
            DELETE FROM ${table} WHERE job_id = ?
        `);
        deleteStmt.run(jobId);

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'deadletter', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true };
    }

    _handleStats(queue) {
        const table = this._getQueueTable(queue);

        const stmt = this.db.prepare(`
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'IN_FLIGHT' THEN 1 ELSE 0 END) as in_flight
            FROM ${table}
        `);
        const row = stmt.get();

        const deadStmt = this.db.prepare(`
            SELECT COUNT(*) as count FROM dead_letter WHERE queue_name = ?
        `);
        const deadRow = deadStmt.get(queue);

        return {
            queue,
            ...row,
            dead_letter: deadRow?.count || 0
        };
    }

    _updateQueueState(queue) {
        try {
            const stats = this._handleStats(queue);
            const stmt = this.db.prepare(`
                INSERT OR REPLACE INTO queue_state 
                (queue_name, pending_count, in_flight_count, dead_letter_count, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            `);
            stmt.run(queue, stats.pending || 0, stats.in_flight || 0, stats.dead_letter || 0);
        } catch (err) {
            // Ignore stats update errors
        }
    }

    shutdown() {
        this.isRunning = false;

        for (const [jobId, { reject }] of this.pendingRequests) {
            reject(new Error('SQLite server shutting down'));
            this.pendingRequests.delete(jobId);
        }

        if (this.db) {
            this.db.close();
        }

        console.log('[SQLiteServer] ✅ Shutdown complete');
        process.exit(0);
    }
}

module.exports = SQLiteServer;