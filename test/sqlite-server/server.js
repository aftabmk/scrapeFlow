// sqlite-server/server.js
const { EventEmitter } = require('events');
const Queue = require('../queue/queue');
const DurableQueue = require('../queue/durable-queue');
const SQLiteWriteWorker = require('../workers/sqlite-write-worker');
const SQLiteReadWorker = require('../workers/sqlite-read-worker');

class SQLiteServer extends EventEmitter {
  constructor(options = {}) {
    super();
    this.dbPath = options.dbPath || './data/queue.db';
    this.writeWorkers = options.writeWorkers || 1;
    this.readWorkers = options.readWorkers || 3;
    this.queues = new Map();
    
    this.writeQueue = new Queue({ name: 'write_queue', maxSize: 10000 });
    this.readQueue = new Queue({ name: 'read_queue', maxSize: 10000 });
    
    this.writeWorker = null;
    this.readWorkersList = [];
    
    this.isRunning = true;
    this.isReady = false;
    this.started = false;
    
    this._setupIPCListener();
  }

  // ✅ start() returns a Promise that resolves when ready
  async start() {
    if (this.started) return this;
    this.started = true;
    
    console.log('[SQLiteServer] Starting...');
    
    // Initialize workers synchronously
    this._initWorkers();
    
    // Set ready flag
    this.isReady = true;
    
    // ✅ EMIT READY IMMEDIATELY (listeners should be attached before start)
    const readyInfo = {
      writeWorkers: this.writeWorkers,
      readWorkers: this.readWorkers,
      queues: Array.from(this.queues.keys()),
      dbPath: this.dbPath,
      pid: process.pid
    };
    
    console.log('[SQLiteServer] Emitting ready event...');
    this.emit('ready', readyInfo);
    
    // Also send via IPC if available
    if (process.send) {
      process.send({ type: 'SQLITE_READY', ...readyInfo });
      console.log('[SQLiteServer] Sent SQLITE_READY via IPC');
    }
    
    return this;
  }

  _setupIPCListener() {
    process.on('message', async (message) => {
      if (!message || !message.type) return;

      try {
        await this._handleRequest(message);
      } catch (error) {
        this._sendResponse(message.requestId, {
          success: false,
          error: error.message
        });
      }
    });
  }

  async _handleRequest(request) {
    const { type, requestId, queueName, data, jobId, workerId, count } = request;

    try {
      if (!this.queues.has(queueName)) {
        console.log(`[SQLiteServer] Creating queue: ${queueName}`);
        const queue = new DurableQueue({
          queueName,
          dbPath: this.dbPath
        });
        await queue.rebuild();
        this.queues.set(queueName, queue);
      }

      const durableQueue = this.queues.get(queueName);

      switch (type) {
        case 'ENQUEUE':
          await this.writeQueue.enqueue({
            type: 'ENQUEUE',
            requestId,
            queueName,
            data,
            durableQueue
          });
          break;

        case 'ACK':
          await this.writeQueue.enqueue({
            type: 'ACK',
            requestId,
            queueName,
            jobId,
            data,
            durableQueue
          });
          break;

        case 'DEQUEUE':
          await this.readQueue.enqueue({
            type: 'DEQUEUE',
            requestId,
            queueName,
            workerId,
            durableQueue
          });
          break;

        case 'DEQUEUE_MULTIPLE':
          await this.readQueue.enqueue({
            type: 'DEQUEUE_MULTIPLE',
            requestId,
            queueName,
            workerId,
            count: count || 1,
            durableQueue
          });
          break;

        case 'GET_STATS':
          await this.readQueue.enqueue({
            type: 'GET_STATS',
            requestId,
            queueName,
            durableQueue
          });
          break;

        default:
          throw new Error(`Unknown request type: ${type}`);
      }
    } catch (error) {
      console.error(`[SQLiteServer] Error handling request:`, error);
      throw error;
    }
  }

  _initWorkers() {
    console.log('[SQLiteServer] Initializing workers...');
    
    this.writeWorker = new SQLiteWriteWorker({
      workerId: 'write_worker_1',
      writeQueue: this.writeQueue
    });

    this.writeWorker.on('result', (response) => {
      this._sendResponse(response.requestId, response);
    });

    for (let i = 0; i < this.readWorkers; i++) {
      const worker = new SQLiteReadWorker({
        workerId: `read_worker_${i + 1}`,
        readQueue: this.readQueue
      });
      
      worker.on('result', (response) => {
        this._sendResponse(response.requestId, response);
      });
      
      this.readWorkersList.push(worker);
    }

    console.log(`[SQLiteServer] Workers initialized: 1 write + ${this.readWorkers} read`);
  }

  _sendResponse(requestId, response) {
    if (!requestId) return;
    
    if (process.send) {
      process.send({
        requestId,
        ...response
      });
    }
  }

  async shutdown() {
    console.log('[SQLiteServer] Shutting down...');
    this.isRunning = false;
    
    if (this.writeWorker) {
      this.writeWorker.shutdown();
    }
    
    for (const worker of this.readWorkersList) {
      worker.shutdown();
    }
    
    for (const [name, queue] of this.queues) {
      try {
        queue.close();
      } catch (err) {
        console.error(`Error closing queue ${name}:`, err);
      }
    }
    
    console.log('[SQLiteServer] Shutdown complete');
    
    if (process.send) {
      process.send({ type: 'SHUTDOWN_COMPLETE' });
    }
    
    process.exit(0);
  }
}

module.exports = SQLiteServer;