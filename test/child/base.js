// child/base.js
const { EventEmitter } = require('events');
const SQLiteCommWorker = require('../workers/sqlite-comm-worker');
const ProcessingWorker = require('../workers/processing-worker');

class BaseChildProcess extends EventEmitter {
  constructor(options = {}) {
    super();
    this.processType = options.processType || 'generic';
    this.queueName = options.queueName || `${this.processType}_queue`;
    this.processingWorkers = options.processingWorkers || 2;
    this.commWorkers = options.commWorkers || 1;
    this.isRunning = true;
    
    this.commWorker = null;
    this.workers = [];
    
    this._initWorkers();
    this._setupIPCListener();
    this._startHeartbeat();
  }

  _initWorkers() {
    this.commWorker = new SQLiteCommWorker({
      workerId: `comm_${process.pid}`,
      processType: this.processType,
      queueName: this.queueName
    });

    for (let i = 0; i < this.processingWorkers; i++) {
      const worker = new ProcessingWorker({
        workerId: `worker_${i}_${process.pid}`,
        sqliteComm: this.commWorker,
        handler: this._getTaskHandler(),
        pollInterval: 1000
      });
      
      worker.on('jobStarted', (data) => this.emit('jobStarted', data));
      worker.on('jobComplete', (data) => this.emit('jobComplete', data));
      worker.on('jobFailed', (data) => this.emit('jobFailed', data));
      
      this.workers.push(worker);
    }

    this.emit('ready', {
      processType: this.processType,
      processingWorkers: this.processingWorkers,
      queueName: this.queueName
    });
  }

  _setupIPCListener() {
    process.on('message', async (message) => {
      if (!message || !message.type) return;

      switch (message.type) {
        case 'NEW_JOB':
          await this._handleNewJob(message);
          break;
        case 'GET_STATUS':
          this._sendStatus();
          break;
        case 'SHUTDOWN':
          await this.shutdown();
          break;
        case 'RESTART':
          await this.restart();
          break;
        case 'SCALE_WORKERS':
          await this._scaleWorkers(message.count);
          break;
        default:
          console.warn(`Unknown message type: ${message.type}`);
      }
    });
  }

  async _handleNewJob(message) {
    try {
      const jobId = await this.commWorker.enqueue({
        ...message.data,
        jobId: message.jobId
      });
      
      process.send({
        type: 'JOB_QUEUED',
        jobId,
        timestamp: Date.now()
      });
    } catch (error) {
      process.send({
        type: 'JOB_ERROR',
        jobId: message.jobId,
        error: error.message
      });
    }
  }

  _getTaskHandler() {
    return async (job) => {
      throw new Error('Task handler not implemented');
    };
  }

  async _scaleWorkers(count) {
    const currentCount = this.workers.length;
    
    if (count > currentCount) {
      for (let i = currentCount; i < count; i++) {
        const worker = new ProcessingWorker({
          workerId: `worker_${i}_${process.pid}`,
          sqliteComm: this.commWorker,
          handler: this._getTaskHandler()
        });
        worker.on('jobStarted', (data) => this.emit('jobStarted', data));
        worker.on('jobComplete', (data) => this.emit('jobComplete', data));
        worker.on('jobFailed', (data) => this.emit('jobFailed', data));
        this.workers.push(worker);
      }
    } else if (count < currentCount) {
      const idleWorkers = this.workers.filter(w => !w.isBusy);
      const toRemove = idleWorkers.slice(0, currentCount - count);
      for (const worker of toRemove) {
        worker.shutdown();
        const idx = this.workers.indexOf(worker);
        if (idx !== -1) {
          this.workers.splice(idx, 1);
        }
      }
    }
  }

  _sendStatus() {
    const workerStatus = this.workers.map(w => w.getStatus());
    process.send({
      type: 'STATUS',
      processType: this.processType,
      commWorker: this.commWorker.workerId,
      workers: workerStatus,
      queueName: this.queueName
    });
  }

  _startHeartbeat() {
    setInterval(() => {
      if (!this.isRunning) return;
      
      process.send({
        type: 'HEARTBEAT',
        pid: process.pid,
        timestamp: Date.now(),
        stats: {
          workers: this.workers.length,
          activeWorkers: this.workers.filter(w => w.isBusy).length,
          queueName: this.queueName
        }
      });
    }, 5000);
  }

  async shutdown() {
    this.isRunning = false;
    
    for (const worker of this.workers) {
      worker.shutdown();
    }
    
    if (this.commWorker) {
      this.commWorker.shutdown();
    }
    
    process.send({ type: 'SHUTDOWN_COMPLETE' });
    process.exit(0);
  }

  async restart() {
    await this.shutdown();
  }

  cleanup() {
    this.isRunning = false;
    for (const worker of this.workers) {
      worker.forceStop();
    }
    if (this.commWorker) {
      this.commWorker.shutdown();
    }
  }
}

module.exports = BaseChildProcess;