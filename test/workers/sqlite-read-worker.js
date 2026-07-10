// workers/sqlite-read-worker.js
const { EventEmitter } = require('events');

class SQLiteReadWorker extends EventEmitter {
  constructor(options = {}) {
    super();
    this.workerId = options.workerId || 'read_worker_1';
    this.readQueue = options.readQueue; // Linked List queue
    this.isRunning = true;
    
    this._startProcessing();
  }

  async _startProcessing() {
    while (this.isRunning) {
      try {
        await this._processRequest();
      } catch (error) {
        console.error(`Read Worker ${this.workerId} error:`, error);
        await this._sleep(100);
      }
    }
  }

  async _processRequest() {
    const request = this.readQueue.dequeue();
    
    if (!request) {
      await this._sleep(10);
      return;
    }

    const { type, requestId, durableQueue, workerId, count } = request;

    try {
      let result;

      switch (type) {
        case 'DEQUEUE':
          const job = await durableQueue.dequeue(workerId);
          result = { job };
          break;

        case 'DEQUEUE_MULTIPLE':
          const jobs = await durableQueue.dequeueMultiple(workerId, count || 1);
          result = { jobs };
          break;

        case 'GET_STATS':
          const stats = await durableQueue.getStats();
          result = { stats };
          break;

        default:
          throw new Error(`Unknown read type: ${type}`);
      }

      this.emit('result', {
        requestId,
        success: true,
        data: result
      });

    } catch (error) {
      this.emit('result', {
        requestId,
        success: false,
        error: error.message
      });
    }
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  shutdown() {
    this.isRunning = false;
    this.emit('shutdown', { workerId: this.workerId });
  }
}

module.exports = SQLiteReadWorker;