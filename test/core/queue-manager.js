// core/queue-manager.js
const { EventEmitter } = require('events');

class QueueManager extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      visibilityTimeout: 30000,
      maxRetries: 3,
      batchSize: 10,
      maxQueueSize: 10000,
      sweepInterval: 1000,
      ...options,
    };
    
    this.queues = new Map();
    this.inFlight = new Map();
    this.deadLetter = [];
    this.completed = new Set();
    this.wal = [];
    this.pendingFlush = [];
    this.persistence = null;
    this.persistenceReady = false;
    
    this.stats = {
      enqueued: 0,
      dequeued: 0,
      completed: 0,
      failed: 0,
      timedOut: 0,
      deadLettered: 0,
    };
    
    this.startSweeper();
    this.startFlusher();
    console.log('[QueueManager] Initialized');
  }

  getQueue(name) {
    if (!this.queues.has(name)) {
      this.queues.set(name, {
        name,
        pending: [],
        processing: new Set(),
        completed: new Set(),
        failed: new Set(),
        stats: { enqueued: 0, dequeued: 0, completed: 0, failed: 0 },
      });
    }
    return this.queues.get(name);
  }

  enqueue(queueName, job, priority = 'normal') {
    const queue = this.getQueue(queueName);
    if (!job.id) job.id = this.generateJobId();
    
    const wrappedJob = {
      ...job,
      queue: queueName,
      priority,
      enqueuedAt: Date.now(),
      attempts: 0,
      maxAttempts: job.maxAttempts || this.options.maxRetries,
      status: 'pending',
      metadata: job.metadata || {},
    };
    
    if (queue.pending.length >= this.options.maxQueueSize) {
      this.emit('queue.full', { queueName, job: wrappedJob });
      return null;
    }
    
    if (priority === 'high') {
      queue.pending.unshift(wrappedJob);
    } else if (priority === 'low') {
      queue.pending.push(wrappedJob);
    } else {
      const insertAt = Math.floor(queue.pending.length / 3);
      queue.pending.splice(insertAt, 0, wrappedJob);
    }
    
    queue.stats.enqueued++;
    this.stats.enqueued++;
    this.wal.push({ op: 'enqueue', queue: queueName, jobId: wrappedJob.id, job: wrappedJob, timestamp: Date.now() });
    this.emit('job.enqueued', { queueName, job: wrappedJob });
    return wrappedJob.id;
  }

  dequeue(queueName, workerId) {
    const queue = this.getQueue(queueName);
    if (queue.pending.length === 0) return null;
    
    const job = queue.pending.shift();
    job.status = 'processing';
    job.dequeuedAt = Date.now();
    job.workerId = workerId;
    job.expiresAt = Date.now() + this.options.visibilityTimeout;
    
    this.inFlight.set(job.id, { job, queue: queueName, workerId, expiresAt: job.expiresAt, retryCount: 0 });
    queue.processing.add(job.id);
    queue.stats.dequeued++;
    this.stats.dequeued++;
    
    this.wal.push({ op: 'dequeue', queue: queueName, jobId: job.id, workerId, timestamp: Date.now() });
    this.emit('job.dequeued', { queueName, jobId: job.id, workerId });
    return job;
  }

  ack(queueName, jobId) {
    const queue = this.getQueue(queueName);
    const inFlight = this.inFlight.get(jobId);
    if (!inFlight) { this.emit('ack.unknown', { queueName, jobId }); return false; }
    
    this.inFlight.delete(jobId);
    queue.processing.delete(jobId);
    queue.completed.add(jobId);
    const job = inFlight.job;
    job.status = 'completed';
    job.completedAt = Date.now();
    queue.stats.completed++;
    this.stats.completed++;
    this.completed.add(jobId);
    this.wal.push({ op: 'ack', queue: queueName, jobId, timestamp: Date.now() });
    this.emit('job.acked', { queueName, jobId });
    return true;
  }

  requeue(queueName, jobId) {
    const inFlight = this.inFlight.get(jobId);
    if (!inFlight) { this.emit('requeue.unknown', { queueName, jobId }); return false; }
    
    const job = inFlight.job;
    job.attempts++;
    
    if (job.attempts >= job.maxAttempts) {
      this.deadLetter.push({ job, queue: queueName, failedAt: Date.now(), attempts: job.attempts, error: job.error || 'Max retries exceeded' });
      this.stats.deadLettered++;
      this.inFlight.delete(jobId);
      const queue = this.getQueue(queueName);
      queue.processing.delete(jobId);
      this.emit('job.deadletter', { queueName, jobId, job });
      return false;
    }
    
    job.status = 'pending';
    job.dequeuedAt = null;
    job.workerId = null;
    job.expiresAt = null;
    this.inFlight.delete(jobId);
    const queue = this.getQueue(queueName);
    queue.processing.delete(jobId);
    queue.pending.unshift(job);
    this.stats.failed++;
    this.emit('job.requeued', { queueName, jobId, attempts: job.attempts });
    return true;
  }

  sweep() {
    const now = Date.now();
    let timedOut = 0;
    for (const [jobId, inFlight] of this.inFlight) {
      if (now > inFlight.expiresAt) {
        timedOut++;
        this.stats.timedOut++;
        this.emit('job.timeout', { queueName: inFlight.queue, jobId });
        this.requeue(inFlight.queue, jobId);
      }
    }
    if (timedOut > 0) console.log(`[QueueManager] Sweep: ${timedOut} jobs timed out`);
  }

  startSweeper() { this.sweeper = setInterval(() => this.sweep(), this.options.sweepInterval); }
  startFlusher() { this.flushInterval = setInterval(() => { if (this.persistenceReady && this.pendingFlush.length > 0) this.flush(); }, 1000); }

  async flush() {
    if (this.pendingFlush.length === 0) return;
    const batch = this.pendingFlush.splice(0, this.options.batchSize);
    if (this.persistence) {
      try {
        await this.persistence.batchWrite(batch);
        this.emit('flush.complete', { count: batch.length });
      } catch (error) {
        console.error('[QueueManager] Flush error:', error);
        this.pendingFlush.unshift(...batch);
      }
    }
  }

  setPersistence(provider) { this.persistence = provider; this.persistenceReady = true; console.log('[QueueManager] Persistence enabled'); }

  getQueueStats(queueName) {
    const queue = this.queues.get(queueName);
    if (!queue) return null;
    const inFlightCount = Array.from(this.inFlight.values()).filter(ifj => ifj.queue === queueName).length;
    return {
      name: queueName,
      pending: queue.pending.length,
      processing: queue.processing.size,
      inFlight: inFlightCount,
      completed: queue.completed.size,
      failed: queue.failed.size,
      deadLetter: this.deadLetter.filter(dl => dl.queue === queueName).length,
      stats: queue.stats,
    };
  }

  getStats() {
    const queueNames = Array.from(this.queues.keys());
    const stats = { queues: {}, total: { ...this.stats }, inFlight: this.inFlight.size, deadLetter: this.deadLetter.length, walSize: this.wal.length, pendingFlush: this.pendingFlush.length };
    for (const name of queueNames) stats.queues[name] = this.getQueueStats(name);
    return stats;
  }

  generateJobId() { return `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`; }

  shutdown() {
    console.log('[QueueManager] Shutting down...');
    clearInterval(this.sweeper);
    clearInterval(this.flushInterval);
    if (this.pendingFlush.length > 0) this.flush();
    this.queues.clear();
    this.inFlight.clear();
    this.deadLetter = [];
    this.completed = new Set();
    this.wal = [];
    this.pendingFlush = [];
    this.removeAllListeners();
    console.log('[QueueManager] Shutdown complete');
  }
}

module.exports = QueueManager;