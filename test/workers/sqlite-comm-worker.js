// workers/sqlite-comm-worker.js
const { EventEmitter } = require('events');

class SQLiteCommWorker extends EventEmitter {
  constructor(options = {}) {
    super();
    this.workerId = options.workerId || `comm_${process.pid}`;
    this.processType = options.processType || 'generic';
    this.queueName = options.queueName || 'default_queue';
    this.requestTimeout = options.requestTimeout || 30000;
    this.pendingRequests = new Map();
    this.isRunning = true;
    
    this._setupIPCListener();
  }

  _setupIPCListener() {
    process.on('message', (message) => {
      if (message && message.type) {
        this._handleResponse(message);
      }
    });
  }

  _handleResponse(response) {
    const { requestId, success, data, error } = response;
    
    if (this.pendingRequests.has(requestId)) {
      const { resolve, reject, timeout } = this.pendingRequests.get(requestId);
      
      clearTimeout(timeout);
      this.pendingRequests.delete(requestId);
      
      if (success) {
        resolve(data);
      } else {
        reject(new Error(error || 'Request failed'));
      }
    }
  }

  _sendRequest(payload) {
    return new Promise((resolve, reject) => {
      const requestId = this._generateRequestId();
      
      const timeout = setTimeout(() => {
        if (this.pendingRequests.has(requestId)) {
          this.pendingRequests.delete(requestId);
          reject(new Error(`Request timeout: ${payload.type}`));
        }
      }, this.requestTimeout);

      this.pendingRequests.set(requestId, { resolve, reject, timeout });
      
      process.send({
        ...payload,
        requestId,
        workerId: this.workerId
      });
    });
  }

  _generateRequestId() {
    return `${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  async enqueue(jobData) {
    return this._sendRequest({
      type: 'ENQUEUE',
      queueName: this.queueName,
      data: jobData
    });
  }

  async ack(jobId, result) {
    return this._sendRequest({
      type: 'ACK',
      queueName: this.queueName,
      jobId,
      data: result
    });
  }

  async dequeue(workerId) {
    return this._sendRequest({
      type: 'DEQUEUE',
      queueName: this.queueName,
      workerId
    });
  }

  async dequeueMultiple(workerId, count) {
    return this._sendRequest({
      type: 'DEQUEUE_MULTIPLE',
      queueName: this.queueName,
      workerId,
      count
    });
  }

  async getStats() {
    return this._sendRequest({
      type: 'GET_STATS',
      queueName: this.queueName
    });
  }

  async getPendingCount() {
    const stats = await this.getStats();
    return stats.pending || 0;
  }

  shutdown() {
    this.isRunning = false;
    for (const [requestId, { reject }] of this.pendingRequests) {
      reject(new Error('Worker shutting down'));
      this.pendingRequests.delete(requestId);
    }
  }
}

module.exports = SQLiteCommWorker;