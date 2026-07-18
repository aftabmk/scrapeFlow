// sqlite/queries.js
class Queries {
  constructor(db) {
    this.db = db;
    this.prepared = {};
  }

  prepare(sql) {
    if (!this.prepared[sql]) this.prepared[sql] = this.db.prepare(sql);
    return this.prepared[sql];
  }

  insertJob(job) {
    const stmt = this.prepare(`INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at, completed_at, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    return stmt.run(job.id, job.queue, job.status, JSON.stringify(job.data), JSON.stringify(job.metadata || {}), job.attempts || 0, job.created_at || Date.now(), Date.now(), job.completed_at || null, job.error || null);
  }

  updateJobStatus(jobId, status, error = null) {
    const stmt = this.prepare(`UPDATE jobs SET status = ?, updated_at = ?, error = ? WHERE id = ?`);
    return stmt.run(status, Date.now(), error, jobId);
  }

  getJob(jobId) {
    const stmt = this.prepare('SELECT * FROM jobs WHERE id = ?');
    const row = stmt.get(jobId);
    if (row) return { ...row, data: JSON.parse(row.data), metadata: JSON.parse(row.metadata || '{}') };
    return null;
  }

  getJobs(queue, status = null, limit = 100, offset = 0) {
    let sql = 'SELECT * FROM jobs WHERE queue = ?';
    const params = [queue];
    if (status) { sql += ' AND status = ?'; params.push(status); }
    sql += ' ORDER BY created_at ASC LIMIT ? OFFSET ?';
    params.push(limit, offset);
    const stmt = this.prepare(sql);
    const rows = stmt.all(...params);
    return rows.map(row => ({ ...row, data: JSON.parse(row.data), metadata: JSON.parse(row.metadata || '{}') }));
  }

  logOperation(queue, jobId, operation, data = null) {
    const stmt = this.prepare(`INSERT INTO queue_log (queue, job_id, operation, data, timestamp) VALUES (?, ?, ?, ?, ?)`);
    return stmt.run(queue, jobId, operation, JSON.stringify(data), Date.now());
  }

  getQueueLog(queue, limit = 100, offset = 0) {
    const stmt = this.prepare(`SELECT * FROM queue_log WHERE queue = ? ORDER BY timestamp DESC LIMIT ? OFFSET ?`);
    const rows = stmt.all(queue, limit, offset);
    return rows.map(row => ({ ...row, data: row.data ? JSON.parse(row.data) : null }));
  }

  addDeadLetter(queue, jobId, jobData, error, attempts) {
    const stmt = this.prepare(`INSERT INTO dead_letter (queue, job_id, job_data, error, attempts, timestamp) VALUES (?, ?, ?, ?, ?, ?)`);
    return stmt.run(queue, jobId, JSON.stringify(jobData), error, attempts || 0, Date.now());
  }

  getStats() {
    const queries = {
      jobs: 'SELECT status, COUNT(*) as count FROM jobs GROUP BY status',
      deadLetter: 'SELECT COUNT(*) as count FROM dead_letter',
      queueLog: 'SELECT COUNT(*) as count FROM queue_log',
      checkpoints: 'SELECT COUNT(*) as count FROM checkpoints',
    };
    const results = {};
    for (const [key, sql] of Object.entries(queries)) {
      const stmt = this.prepare(sql);
      results[key] = stmt.all();
    }
    return results;
  }

  cleanup(ageMs = 7 * 24 * 60 * 60 * 1000) {
    const cutoff = Date.now() - ageMs;
    const stmt = this.prepare(`DELETE FROM jobs WHERE status = 'completed' AND updated_at < ?`);
    const deleted = stmt.run(cutoff);
    const logStmt = this.prepare(`DELETE FROM queue_log WHERE timestamp < ?`);
    logStmt.run(cutoff);
    return deleted.changes || 0;
  }

  getWALEntries(queue, since = null) {
    let sql = 'SELECT * FROM queue_log WHERE queue = ?';
    const params = [queue];
    if (since) { sql += ' AND timestamp > ?'; params.push(since); }
    sql += ' ORDER BY timestamp ASC';
    const stmt = this.prepare(sql);
    const rows = stmt.all(...params);
    return rows.map(row => ({ operation: row.operation, jobId: row.job_id, data: row.data ? JSON.parse(row.data) : null, timestamp: row.timestamp }));
  }

  tableExists(tableName) {
    const stmt = this.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`);
    const result = stmt.get(tableName);
    return result !== undefined && result !== null;
  }

  getDBSize() {
    const stmt = this.prepare('SELECT page_count * page_size as size FROM pragma_page_count, pragma_page_size');
    const result = stmt.get();
    return result ? result.size : 0;
  }
}

module.exports = Queries;