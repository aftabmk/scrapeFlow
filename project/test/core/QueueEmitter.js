'use strict';

const EventEmitter = require('events');
const DurableQueue = require('../../algorithms/durableQueue');

class QueueEmitter extends EventEmitter {
  constructor(workerHandle, opts = {}) {
    super();

    this.worker = workerHandle;

    this.queue = new DurableQueue(opts.name || 'queue', {
      visibilityTimeout: opts.visibilityTimeout ?? 1,
      maxRetries: opts.maxRetries ?? 3,
    });

    this.running = false;

    this.worker.on('message', (msg) => {
      this._onWorkerMessage(msg).catch(err => this.emit('error', err));
    });

    this.on('task', () => {
      this._pump().catch(err => this.emit('error', err));
    });
  }

  start() {
    this.running = true;
    this._pump().catch(err => this.emit('error', err));
  }

  stop() {
    this.running = false;
  }

  async enqueue(job) {
    if (!job?.id) {
      throw new Error('QueueEmitter.enqueue: job must have an id');
    }

    const jobObj = { id : job.id, job, enteredAt : Date.now()};

    await this.queue.enqueue(job);

    this.emit('task');
  }

  async _pump() {
    if (!this.running) return;

    while (this.running) {
      const batch = await this.queue.dequeueBatch(1);

      if (!batch.length) {
        break;
      }

      this.worker.postMessage({
        job: batch[0],
      });
    }
  }

  async _onWorkerMessage(msg) {
    await this.queue.ack(msg.job.id);

    this.emit('result', {
      ok: msg.ok,
      job: msg.job,
      result: msg.result,
      error: msg.error,
      trace: msg.trace,
    });

    this.emit('task');
  }
}

module.exports = { QueueEmitter };