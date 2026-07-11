// child/components/queue-manager.js
const DurableQueue = require('../../queue/durable-queue');

class QueueManager {
  constructor(options = {}) {
    this.queueName = options.queueName || 'default_queue';
    this.dbPath = options.dbPath || './data/queue.db';
    this.enabled = options.enabled !== false;
    this.queue = null;
    this.isRunning = false;
  }

  start() {
    if (!this.enabled) {
      console.log(`[QueueManager] ⏳ Disabled for ${this.queueName}`);
      return;
    }

    this.queue = new DurableQueue({
      queueName: this.queueName,
      dbPath: this.dbPath
    });

    this.isRunning = true;
    console.log(`[QueueManager] ✅ Started: ${this.queueName}`);
  }

  async enqueue(data) {
    if (!this.enabled || !this.queue) {
      throw new Error('Queue not available');
    }
    return this.queue.enqueue(data);
  }

  async dequeue(workerId) {
    if (!this.enabled || !this.queue) {
      return null;
    }
    return this.queue.dequeue(workerId);
  }

  async ack(jobId, result) {
    if (!this.enabled || !this.queue) {
      return;
    }
    return this.queue.ack(jobId, result);
  }

  getSize() {
    if (!this.enabled || !this.queue) {
      return 0;
    }
    return this.queue.memoryCache?.pending?.length || 0;
  }

  getStats() {
    if (!this.enabled || !this.queue) {
      return { pending: 0, inProgress: 0, total: 0 };
    }
    return this.queue.getStats();
  }

  async close() {
    if (this.queue) {
      this.queue.close();
      this.queue = null;
    }
    this.isRunning = false;
    console.log(`[QueueManager] ✅ Closed: ${this.queueName}`);
  }
}

module.exports = QueueManager;