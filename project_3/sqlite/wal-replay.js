// sqlite/wal-replay.js
class WALReplay {
  constructor(db) {
    this.db = db;
    this.cache = [];
    this.maxCache = 1000;
  }

  replay(queue, since = null) {
    let sql = 'SELECT * FROM queue_log WHERE queue = ?';
    const params = [queue];
    if (since) { sql += ' AND timestamp > ?'; params.push(since); }
    sql += ' ORDER BY timestamp ASC';
    const stmt = this.db.prepare(sql);
    const rows = stmt.all(...params);
    return rows.map(row => ({
      operation: row.operation,
      jobId: row.job_id,
      data: row.data ? JSON.parse(row.data) : null,
      timestamp: row.timestamp,
    }));
  }

  reconstructState(queue) {
    const entries = this.replay(queue);
    const state = {};
    for (const entry of entries) {
      switch (entry.operation) {
        case 'enqueue':
          state[entry.jobId] = { id: entry.jobId, status: 'pending', data: entry.data, updatedAt: entry.timestamp };
          break;
        case 'dequeue':
          if (state[entry.jobId]) { state[entry.jobId].status = 'processing'; state[entry.jobId].updatedAt = entry.timestamp; }
          break;
        case 'ack':
          if (state[entry.jobId]) { state[entry.jobId].status = 'completed'; state[entry.jobId].updatedAt = entry.timestamp; }
          break;
        case 'requeue':
          if (state[entry.jobId]) { state[entry.jobId].status = 'pending'; state[entry.jobId].updatedAt = entry.timestamp; state[entry.jobId].attempts = (state[entry.jobId].attempts || 0) + 1; }
          break;
        case 'deadletter':
          if (state[entry.jobId]) { state[entry.jobId].status = 'deadletter'; state[entry.jobId].updatedAt = entry.timestamp; }
          break;
      }
    }
    return state;
  }

  writeEntry(queue, jobId, operation, data = null) {
    const stmt = this.db.prepare(`INSERT INTO queue_log (queue, job_id, operation, data, timestamp) VALUES (?, ?, ?, ?, ?)`);
    stmt.run(queue, jobId, operation, JSON.stringify(data), Date.now());
    this.cache.push({ queue, jobId, operation, data, timestamp: Date.now() });
    if (this.cache.length > this.maxCache) this.cache.shift();
  }

  getSize() {
    const stmt = this.db.prepare('SELECT COUNT(*) as count FROM queue_log');
    const result = stmt.get();
    return result ? result.count : 0;
  }

  cleanup(ageMs = 7 * 24 * 60 * 60 * 1000) {
    const cutoff = Date.now() - ageMs;
    const stmt = this.db.prepare('DELETE FROM queue_log WHERE timestamp < ?');
    const result = stmt.run(cutoff);
    return result.changes || 0;
  }

  clearCache() { this.cache = []; }

  getStats() {
    return { size: this.getSize(), cacheSize: this.cache.length, maxCache: this.maxCache };
  }
}

module.exports = WALReplay;