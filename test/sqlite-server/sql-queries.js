// sqlite-server/sql-queries.js
class SQLQueries {
    constructor(db) {
        this.db = db;
    }

    // === WRITE OPERATIONS ===

    append(queue, jobId, payload) {
        const table = `queue_${queue}`;
        
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
        return { success: true, jobId };
    }

    deliver(queue, jobId) {
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            UPDATE ${table} 
            SET status = 'IN_FLIGHT',
                visible_at = DATETIME(CURRENT_TIMESTAMP, '+30 seconds'),
                updated_at = CURRENT_TIMESTAMP
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
        return { success: true, jobId };
    }

    ack(queue, jobId) {
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            DELETE FROM ${table} WHERE job_id = ?
        `);
        const result = stmt.run(jobId);

        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found for ACK`);
        }

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'ack', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true, jobId };
    }

    requeue(queue, jobId) {
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            UPDATE ${table} 
            SET status = 'PENDING', 
                retries = retries + 1,
                updated_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
        `);
        const result = stmt.run(jobId);

        if (result.changes === 0) {
            throw new Error(`Job ${jobId} not found for requeue`);
        }

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'requeue', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true, jobId };
    }

    deadletter(queue, jobId, payload) {
        const stmt = this.db.prepare(`
            INSERT INTO dead_letter (queue_name, job_id, payload, retries)
            VALUES (?, ?, ?, ?)
        `);
        stmt.run(queue, jobId, JSON.stringify(payload), payload?.retries || 0);

        const table = `queue_${queue}`;
        const deleteStmt = this.db.prepare(`
            DELETE FROM ${table} WHERE job_id = ?
        `);
        deleteStmt.run(jobId);

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'deadletter', '${jobId}')
        `);

        this._updateQueueState(queue);
        return { success: true, jobId };
    }

    // === READ OPERATIONS ===

    dequeue(queue, workerId) {
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            SELECT job_id, payload FROM ${table}
            WHERE status = 'PENDING'
            ORDER BY created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        `);
        const row = stmt.get();

        if (!row) {
            return { job: null };
        }

        const updateStmt = this.db.prepare(`
            UPDATE ${table} 
            SET status = 'IN_FLIGHT',
                visible_at = DATETIME(CURRENT_TIMESTAMP, '+30 seconds'),
                worker_id = ?,
                started_at = CURRENT_TIMESTAMP
            WHERE job_id = ?
        `);
        updateStmt.run(workerId, row.job_id);

        this.db.exec(`
            INSERT INTO queue_log (queue_name, op, job_id)
            VALUES ('${queue}', 'deliver', '${row.job_id}')
        `);

        this._updateQueueState(queue);
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
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            SELECT job_id, payload FROM ${table} 
            WHERE status IN ('PENDING', 'IN_FLIGHT')
            ORDER BY created_at ASC
        `);
        const rows = stmt.all();

        console.log(`[SQLQueries] 🔄 Recovered ${rows.length} jobs for ${queue}`);

        return {
            rows: rows.map(row => ({
                id: row.job_id,
                payload: JSON.parse(row.payload),
                op: 'append'
            }))
        };
    }

    stats(queue) {
        const table = `queue_${queue}`;

        const stmt = this.db.prepare(`
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'IN_FLIGHT' THEN 1 ELSE 0 END) as in_flight,
                SUM(CASE WHEN status = 'COMPLETE' THEN 1 ELSE 0 END) as complete,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
            FROM ${table}
        `);
        const row = stmt.get();

        const deadStmt = this.db.prepare(`
            SELECT COUNT(*) as count FROM dead_letter WHERE queue_name = ?
        `);
        const deadRow = deadStmt.get(queue);

        return {
            total: row.total || 0,
            pending: row.pending || 0,
            in_flight: row.in_flight || 0,
            complete: row.complete || 0,
            failed: row.failed || 0,
            dead_letter: deadRow?.count || 0,
            queue
        };
    }

    getOrCreateTable(queueName) {
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

        return tableName;
    }

    // === HELPER ===

    _updateQueueState(queue) {
        try {
            const stats = this.stats(queue);
            const updateStmt = this.db.prepare(`
                INSERT OR REPLACE INTO queue_state 
                (queue_name, pending_count, in_flight_count, dead_letter_count, last_updated)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            `);
            updateStmt.run(queue, stats.pending || 0, stats.in_flight || 0, stats.dead_letter || 0);
        } catch (err) {
            // Ignore stats update errors
        }
    }
}

module.exports = SQLQueries;