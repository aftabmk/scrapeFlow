const { LFUCache } = require('../../../../../algorithms/LFUCache/algorithms/LFUCahche');

class QueueManager {
  constructor(maxTabs) {
    this.maxTabs = maxTabs;
    this.cache = new LFUCache(maxTabs);
    this.queue = [];
  }

  get(jobId) {
    return this.cache.get(jobId);
  }

  add(jobId, tab) {
    this.cache.set(jobId, tab);
  }

  remove(jobId) {
    this.cache.delete(jobId);
  }

  hasCapacity() {
    return this.cache.size < this.maxTabs;
  }

  enqueue(job) {
    this.queue.push(job);
  }

  dequeue() {
    return this.queue.shift();
  }

  get hasQueuedJobs() {
    return this.queue.length > 0;
  }
}

module.exports = QueueManager;