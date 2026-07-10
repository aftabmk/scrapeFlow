// workers/sqlite-write-worker.js
const { EventEmitter } = require('events');

class SQLiteWriteWorker extends EventEmitter {
  constructor(options = {}) {
    super();
    this.workerId = options.workerId || 'write_worker_1';
    this.writeQueue = options.writeQueue; // Linked List queue
    this.isRunning = true;
    this.batchSize = 10;
    this.batchTimeout = 100;
    
    this._startProcessing();
  }

  async _startProcessing() {
    while (this.isRunning) {
      try {
        await this._processBatch();
      } catch (error) {
        console.error(`Write Worker error:`, error);
        await this._sleep(100);
      }
    }
  }

  async _processBatch() {
    const batch = [];
    let batchStart = Date.now();
    
    // Use linked list queue - dequeue multiple for batch
    while (batch.length < this.batchSize && (Date.now() - batchStart) < this.batchTimeout) {
      const request = this.writeQueue.dequeue();
      if (request) {
        batch.push(request);
      } else {
        await this._sleep(10);
      }
    }

    if (batch.length === 0) {
      await this._sleep(50);
      return;
    }

    for (const request of batch) {
      if (!this.isRunning) break;
      await this._processRequest(request);
    }
  }

  async _processRequest(request) {
    const { type, requestId, durableQueue, data, jobId } = request;

    try {
      let result;

      switch (type) {
        case 'ENQUEUE':
          const jobIdResult = await durableQueue.enqueue(data);
          result = { jobId: jobIdResult, status: 'queued' };
          break;

        case 'ACK':
          await durableQueue.ack(jobId, data);
          result = { jobId, status: 'acknowledged' };
          break;

        default:
          throw new Error(`Unknown write type: ${type}`);
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

module.exports = SQLiteWriteWorker;