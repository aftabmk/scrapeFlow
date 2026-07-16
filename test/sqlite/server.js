// sqlite/server.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');
const Queries = require('./queries');
const WALReplay = require('./wal-replay');

class SQLiteServer extends EventEmitter {
  constructor(options = {}) {
    super();

    this.options = {
      dbPath: options.dbPath || './data/queue.db',
      readWorkers: options.readWorkers || 2,
      writeWorkers: options.writeWorkers || 2,
      batchSize: options.batchSize || 50,
      cacheSize: options.cacheSize || 2000,
      checkpointInterval: options.checkpointInterval || 5000,
      ...options,
    };

    this.db = null;
    this.queries = null;
    this.wal = null;
    this.isReady = false;
    this.isRunning = false;
    this.startTime = null;
    this.tablesCreated = false;
    this.pendingWrites = [];
    this.writeInterval = null;
    this.checkpointInterval = null;
    this.cache = new Map();

    console.log('[SQLite] Server initializing...');
  }

  async start() {
    this.isRunning = true;
    this.startTime = Date.now();

    try {
      this.initDatabase();
      this.queries = new Queries(this.db);
      this.wal = new WALReplay(this.db);
      await this.createTables();
      this.startWriteBatching();
      this.startCheckpointing();
      this.isReady = true;

      this.emit('ready', { dbPath: this.options.dbPath, readWorkers: this.options.readWorkers, writeWorkers: this.options.writeWorkers, timestamp: Date.now() });
      console.log('[SQLite] Server ready');
    } catch (error) {
      console.error('[SQLite] Start error:', error);
      this.emit('error', error);
      throw error;
    }
  }

  initDatabase() {
    try {
      const dir = path.dirname(this.options.dbPath);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      this.db = new DatabaseSync(this.options.dbPath);
      this.db.exec('PRAGMA journal_mode=WAL');
      this.db.exec(`PRAGMA synchronous=${this.options.syncMode || 'NORMAL'}`);
      this.db.exec(`PRAGMA cache_size=${this.options.cacheSize || 2000}`);
      this.db.exec('PRAGMA busy_timeout=5000');
      this.db.exec('PRAGMA foreign_keys=ON');
      console.log('[SQLite] Database initialized');
    } catch (error) {
      console.error('[SQLite] Init error:', error);
      throw error;
    }
  }

  async createTables() {
    try {
      this.db.exec(`CREATE TABLE IF NOT EXISTS jobs (id TEXT PRIMARY KEY, queue TEXT NOT NULL, status TEXT NOT NULL, data TEXT NOT NULL, metadata TEXT, attempts INTEGER DEFAULT 0, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, completed_at INTEGER, error TEXT)`);
      this.db.exec(`CREATE TABLE IF NOT EXISTS queue_log (id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL, job_id TEXT NOT NULL, operation TEXT NOT NULL, data TEXT, timestamp INTEGER NOT NULL)`);
      this.db.exec(`CREATE TABLE IF NOT EXISTS checkpoints (id TEXT PRIMARY KEY, state TEXT NOT NULL, timestamp INTEGER NOT NULL)`);
      this.db.exec(`CREATE TABLE IF NOT EXISTS dead_letter (id INTEGER PRIMARY KEY AUTOINCREMENT, queue TEXT NOT NULL, job_id TEXT NOT NULL, job_data TEXT NOT NULL, error TEXT, attempts INTEGER DEFAULT 0, timestamp INTEGER NOT NULL)`);
      this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_queue ON jobs(queue, status)`);
      this.db.exec(`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, created_at)`);
      this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_queue ON queue_log(queue, timestamp)`);
      this.db.exec(`CREATE INDEX IF NOT EXISTS idx_queue_log_job ON queue_log(job_id)`);
      this.tablesCreated = true;
      console.log('[SQLite] Tables created');
    } catch (error) {
      console.error('[SQLite] Table creation error:', error);
      throw error;
    }
  }

  startWriteBatching() {
    this.writeInterval = setInterval(() => {
      if (this.pendingWrites.length > 0) this.flushWrites();
    }, 500);
  }

  async flushWrites() {
    if (this.pendingWrites.length === 0) return;
    const batch = this.pendingWrites.splice(0, this.options.batchSize);

    try {
      const stmt = this.db.prepare(`INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at, completed_at, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      this.db.exec('BEGIN TRANSACTION');
      for (const write of batch) {
        stmt.run(write.id, write.queue, write.status, JSON.stringify(write.data), JSON.stringify(write.metadata || {}), write.attempts || 0, write.created_at || Date.now(), Date.now(), write.completed_at || null, write.error || null);
        if (write.operation) {
          const logStmt = this.db.prepare(`INSERT INTO queue_log (queue, job_id, operation, data, timestamp) VALUES (?, ?, ?, ?, ?)`);
          logStmt.run(write.queue, write.id, write.operation, JSON.stringify(write.data), Date.now());
        }
      }
      this.db.exec('COMMIT');
      this.emit('flush.complete', { count: batch.length });
    } catch (error) {
      console.error('[SQLite] Flush error:', error);
      this.pendingWrites.unshift(...batch);
      this.emit('flush.error', error);
    }
  }

  writeJob(job) {
    if (!this.isReady) { this.pendingWrites.push(job); return; }
    this.pendingWrites.push(job);
    if (this.pendingWrites.length >= this.options.batchSize * 2) this.flushWrites();
  }

  readJob(jobId) {
    if (this.cache.has(jobId)) return this.cache.get(jobId);
    const stmt = this.db.prepare('SELECT * FROM jobs WHERE id = ?');
    const row = stmt.get(jobId);
    if (row) {
      const job = { ...row, data: JSON.parse(row.data), metadata: JSON.parse(row.metadata || '{}') };
      this.cache.set(jobId, job);
      return job;
    }
    return null;
  }

  readQueueJobs(queue, status = null, limit = 100) {
    let query = 'SELECT * FROM jobs WHERE queue = ?';
    const params = [queue];
    if (status) { query += ' AND status = ?'; params.push(status); }
    query += ' ORDER BY created_at ASC LIMIT ?';
    params.push(limit);
    const stmt = this.db.prepare(query);
    const rows = stmt.all(...params);
    return rows.map(row => ({ ...row, data: JSON.parse(row.data), metadata: JSON.parse(row.metadata || '{}') }));
  }

  saveCheckpoint(checkpoint) {
    const stmt = this.db.prepare(`INSERT OR REPLACE INTO checkpoints (id, state, timestamp) VALUES (?, ?, ?)`);
    stmt.run(checkpoint.id, JSON.stringify(checkpoint.state), checkpoint.timestamp);
  }

  loadCheckpoint() {
    const stmt = this.db.prepare(`SELECT * FROM checkpoints ORDER BY timestamp DESC LIMIT 1`);
    const row = stmt.get();
    if (row) return { id: row.id, state: JSON.parse(row.state), timestamp: row.timestamp };
    return null;
  }

  startCheckpointing() {
    this.checkpointInterval = setInterval(() => this.emit('checkpoint.request'), this.options.checkpointInterval);
  }

  async batchWrite(entries) {
    if (!this.isReady) { for (const entry of entries) this.pendingWrites.push(entry); return; }
    try {
      const stmt = this.db.prepare(`INSERT OR REPLACE INTO jobs (id, queue, status, data, metadata, attempts, created_at, updated_at, completed_at, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`);
      this.db.exec('BEGIN TRANSACTION');
      for (const entry of entries) {
        stmt.run(entry.id, entry.queue, entry.status, JSON.stringify(entry.data), JSON.stringify(entry.metadata || {}), entry.attempts || 0, entry.created_at || Date.now(), Date.now(), entry.completed_at || null, entry.error || null);
      }
      this.db.exec('COMMIT');
      this.emit('batch.complete', { count: entries.length });
    } catch (error) {
      console.error('[SQLite] Batch write error:', error);
      this.emit('batch.error', error);
      throw error;
    }
  }

  getStats() {
    const jobCount = this.db.prepare('SELECT COUNT(*) as count FROM jobs').get();
    const queueLogCount = this.db.prepare('SELECT COUNT(*) as count FROM queue_log').get();
    const checkpointCount = this.db.prepare('SELECT COUNT(*) as count FROM checkpoints').get();
    const deadLetterCount = this.db.prepare('SELECT COUNT(*) as count FROM dead_letter').get();
    return { ready: this.isReady, running: this.isRunning, tablesCreated: this.tablesCreated, jobCount: jobCount.count, queueLogCount: queueLogCount.count, checkpointCount: checkpointCount.count, deadLetterCount: deadLetterCount.count, pendingWrites: this.pendingWrites.length, cacheSize: this.cache.size, uptime: Date.now() - this.startTime };
  }

  // sqlite/server.js - Updated shutdown

  shutdown() {
    console.log('[SQLite] Shutting down...');

    this.isRunning = false;

    // ✅ Clear intervals
    if (this.writeInterval) {
      clearInterval(this.writeInterval);
      this.writeInterval = null;
    }

    if (this.checkpointInterval) {
      clearInterval(this.checkpointInterval);
      this.checkpointInterval = null;
    }

    // ✅ Flush pending writes
    if (this.pendingWrites.length > 0) {
      console.log(`[SQLite] Flushing ${this.pendingWrites.length} pending writes...`);
      try {
        this.flushWrites();
      } catch (error) {
        console.error('[SQLite] Flush error during shutdown:', error);
      }
    }

    // ✅ Clear cache
    this.cache.clear();

    // ✅ Close database with timeout
    if (this.db) {
      try {
        console.log('[SQLite] Closing database...');
        // ✅ Try to close, but don't wait forever
        Promise.race([
          new Promise((resolve) => {
            try {
              this.db.close();
              resolve();
            } catch (e) {
              resolve();
            }
          }),
          new Promise((resolve) => setTimeout(resolve, 2000))
        ]).then(() => {
          console.log('[SQLite] Database closed');
        });
      } catch (error) {
        console.error('[SQLite] Error closing database:', error);
      }
    }

    this.isReady = false;
    console.log('[SQLite] Shutdown complete');
    this.emit('shutdown');
  }
}

module.exports = SQLiteServer;