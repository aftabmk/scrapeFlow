// queue/durable-queue.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');

class DurableQueue {
  constructor(options = {}) {
    this.queueName = options.queueName || 'default_queue';
    this.dbPath = options.dbPath || path.join(__dirname, '../data/queue.db');
    this.visibilityTimeout = options.visibilityTimeout || 30;
    this.maxRetries = options.maxRetries || 3;
    
    this.db = null;
    this.isConnected = false;
    this.tableName = `queue_${this.queueName}`;
    
    this.memoryCache = {
      pending: [],
      inProgress: new Map()
    };
    this.isRebuilding = false;
    
    this.stats = {
      totalEnqueued: 0,
      totalDequeued: 0,
      totalAcked: 0,
      totalTimeout: 0,
      totalRebuilt: 0
    };

    this._initDB();
    this._startTimeoutMonitor();
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
      CREATE TABLE IF NOT EXISTS ${this.tableName} (
        job_id TEXT PRIMARY KEY,
        data TEXT NOT NULL,
        status TEXT CHECK(status IN ('PENDING', 'IN_PROGRESS', 'COMPLETE', 'FAILED')),
        priority INTEGER DEFAULT 0,
        visible_at DATETIME,
        retry_count INTEGER DEFAULT 0,
        max_retries INTEGER DEFAULT 3,
        worker_id TEXT,
        result TEXT,
        error TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        started_at DATETIME,
        completed_at DATETIME
      )
    `);

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_${this.queueName}_status 
      ON ${this.tableName}(status, visible_at, priority DESC, created_at)
    `);

    this.db.exec(`
      CREATE INDEX IF NOT EXISTS idx_${this.queueName}_worker 
      ON ${this.tableName}(worker_id, status)
    `);

    this.isConnected = true;
    
    this.rebuild().catch(err => {
      console.error(`[DurableQueue] Failed to rebuild ${this.queueName}:`, err);
    });
  }

  async rebuild() {
    if (this.isRebuilding) return;
    this.isRebuilding = true;

    console.log(`[DurableQueue] Rebuilding ${this.queueName}...`);

    try {
      const stmt = this.db.prepare(`
        SELECT * FROM ${this.tableName} 
        WHERE status IN ('PENDING', 'IN_PROGRESS') 
        ORDER BY priority DESC, created_at ASC
      `);
      
      const rows = stmt.all();

      this.memoryCache.pending = [];
      this.memoryCache.inProgress = new Map();

      let pendingCount = 0;
      let inProgressCount = 0;
      let requeuedCount = 0;

      for (const row of rows) {
        const job = {
          ...row,
          data: JSON.parse(row.data),
          result: row.result ? JSON.parse(row.result) : null
        };

        if (row.status === 'PENDING') {
          this.memoryCache.pending.push(job);
          pendingCount++;
        } else if (row.status === 'IN_PROGRESS') {
          const visibleAt = new Date(row.visible_at);
          const now = new Date();
          
          if (visibleAt <= now) {
            const updateStmt = this.db.prepare(`
              UPDATE ${this.tableName}
              SET status = 'PENDING',
                  visible_at = CURRENT_TIMESTAMP,
                  retry_count = retry_count + 1,
                  worker_id = NULL,
                  started_at = NULL
              WHERE job_id = ?
            `);
            updateStmt.run(row.job_id);
            
            const requeuedJob = {
              ...job,
              status: 'PENDING',
              worker_id: null,
              retry_count: (row.retry_count || 0) + 1,
              started_at: null
            };
            this.memoryCache.pending.push(requeuedJob);
            requeuedCount++;
            this.stats.totalTimeout++;
          } else {
            this.memoryCache.inProgress.set(row.job_id, job);
            inProgressCount++;
          }
        }
      }

      this.memoryCache.pending.sort((a, b) => {
        if (a.priority !== b.priority) return b.priority - a.priority;
        return new Date(a.created_at) - new Date(b.created_at);
      });

      this.stats.totalRebuilt = rows.length;
      this.isRebuilding = false;

      console.log(`[DurableQueue] ${this.queueName} rebuilt: ${pendingCount} pending, ${inProgressCount} in-progress, ${requeuedCount} requeued`);

      return {
        pending: pendingCount,
        inProgress: inProgressCount,
        requeued: requeuedCount,
        total: rows.length
      };

    } catch (error) {
      this.isRebuilding = false;
      throw error;
    }
  }

  async enqueue(jobData) {
    const jobId = jobData.jobId || this._generateJobId();
    
    try {
      const stmt = this.db.prepare(`
        INSERT INTO ${this.tableName} 
        (job_id, data, status, priority, visible_at, max_retries)
        VALUES (?, ?, 'PENDING', ?, CURRENT_TIMESTAMP, ?)
      `);
      stmt.run(jobId, JSON.stringify(jobData), jobData.priority || 0, this.maxRetries);

      this.memoryCache.pending.push({
        job_id: jobId,
        data: jobData,
        status: 'PENDING',
        priority: jobData.priority || 0,
        visible_at: new Date().toISOString(),
        retry_count: 0,
        max_retries: this.maxRetries,
        created_at: new Date().toISOString()
      });
      
      this.memoryCache.pending.sort((a, b) => {
        if (a.priority !== b.priority) return b.priority - a.priority;
        return new Date(a.created_at) - new Date(b.created_at);
      });
      
      this.stats.totalEnqueued++;
      return jobId;

    } catch (error) {
      throw error;
    }
  }

  async dequeue(workerId) {
    if (this.memoryCache.pending.length > 0) {
      const job = this.memoryCache.pending.shift();
      
      try {
        const stmt = this.db.prepare(`
          UPDATE ${this.tableName}
          SET status = 'IN_PROGRESS',
              visible_at = DATETIME(CURRENT_TIMESTAMP, '+' || ? || ' seconds'),
              worker_id = ?,
              started_at = CURRENT_TIMESTAMP
          WHERE job_id = ? AND status = 'PENDING'
        `);
        const result = stmt.run(this.visibilityTimeout, workerId, job.job_id);
        
        if (result.changes === 0) {
          this.memoryCache.pending.unshift(job);
          return null;
        }

        const inProgressJob = { ...job, status: 'IN_PROGRESS', worker_id: workerId };
        this.memoryCache.inProgress.set(job.job_id, inProgressJob);
        this.stats.totalDequeued++;
        return inProgressJob;

      } catch (error) {
        this.memoryCache.pending.unshift(job);
        throw error;
      }
    }

    try {
      const stmt = this.db.prepare(`
        SELECT * FROM ${this.tableName}
        WHERE status = 'PENDING'
          AND visible_at <= CURRENT_TIMESTAMP
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
      `);
      const row = stmt.get();

      if (!row) return null;

      const updateStmt = this.db.prepare(`
        UPDATE ${this.tableName}
        SET status = 'IN_PROGRESS',
            visible_at = DATETIME(CURRENT_TIMESTAMP, '+' || ? || ' seconds'),
            worker_id = ?,
            started_at = CURRENT_TIMESTAMP
        WHERE job_id = ?
      `);
      updateStmt.run(this.visibilityTimeout, workerId, row.job_id);

      const job = {
        ...row,
        data: JSON.parse(row.data),
        result: row.result ? JSON.parse(row.result) : null
      };
      
      this.memoryCache.inProgress.set(job.job_id, job);
      this.stats.totalDequeued++;
      return job;

    } catch (error) {
      throw error;
    }
  }

  async ack(jobId, result) {
    try {
      const stmt = this.db.prepare(`
        UPDATE ${this.tableName}
        SET status = 'COMPLETE',
            completed_at = CURRENT_TIMESTAMP,
            result = ?
        WHERE job_id = ? AND status = 'IN_PROGRESS'
      `);
      const resultObj = stmt.run(JSON.stringify(result), jobId);
      
      if (resultObj.changes > 0) {
        this.memoryCache.inProgress.delete(jobId);
        this.stats.totalAcked++;
        return true;
      }
      return false;

    } catch (error) {
      throw error;
    }
  }

  async getStats() {
    try {
      const stmt = this.db.prepare(`
        SELECT 
          COUNT(*) as total,
          SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) as pending,
          SUM(CASE WHEN status='IN_PROGRESS' THEN 1 ELSE 0 END) as inProgress,
          SUM(CASE WHEN status='COMPLETE' THEN 1 ELSE 0 END) as complete,
          SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) as failed
        FROM ${this.tableName}
      `);
      const row = stmt.get();

      return {
        ...row,
        ...this.stats,
        memoryCache: {
          pending: this.memoryCache.pending.length,
          inProgress: this.memoryCache.inProgress.size
        }
      };

    } catch (error) {
      throw error;
    }
  }

  _generateJobId() {
    return `${this.queueName}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  _startTimeoutMonitor() {
    setInterval(async () => {
      try {
        await this._processTimeouts();
      } catch (err) {
        console.error(`[DurableQueue] Timeout monitor error for ${this.queueName}:`, err);
      }
    }, 5000);
  }

  async _processTimeouts() {
    try {
      const stmt = this.db.prepare(`
        UPDATE ${this.tableName}
        SET status = CASE 
          WHEN retry_count >= max_retries THEN 'FAILED'
          ELSE 'PENDING'
        END,
        visible_at = CURRENT_TIMESTAMP,
        retry_count = retry_count + 1,
        worker_id = NULL,
        started_at = NULL
        WHERE status = 'IN_PROGRESS'
          AND visible_at <= CURRENT_TIMESTAMP
      `);
      const result = stmt.run();
      
      if (result.changes > 0) {
        this.stats.totalTimeout += result.changes;
      }
      
      return result.changes;

    } catch (error) {
      throw error;
    }
  }

  close() {
    if (this.db) {
      this.db.close();
      this.isConnected = false;
    }
  }
}

module.exports = DurableQueue;