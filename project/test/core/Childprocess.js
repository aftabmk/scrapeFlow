'use strict';

const { Worker } = require('worker_threads');
const path = require('path');
const ProcessBase = require('./ProcessBase');
const { QueueEmitter } = require('./QueueEmitter');


class ChildProcess extends ProcessBase {

  constructor(opts) {
    super({ name: opts.name });
    this.handlerPath = opts.handlerPath;
    this.forwardTo = Array.isArray(opts.forwardTo) ? opts.forwardTo : [];

    this.worker = new Worker(path.join(__dirname, 'WorkerThreadWrapper.js'), {
      workerData: { handlerPath: this.handlerPath, handlerOptions: opts.handlerOptions },
    });

    this.queueEmitter = new QueueEmitter(this.worker, { name : opts.name , concurrency: opts.concurrency });
    this.queueEmitter.on('result', (payload) => this._onResult(payload));

    this.queueEmitter.start();
    this.signalReady();
  }

  onMessage(msg) {
    if (!msg || typeof msg.type !== 'string') return;

    if (msg.type.startsWith('ctrl:')) {
      // lifecycle messages: ctrl:ready, ctrl:shutdown, etc.
      return;
    }

    if (msg.type === 'job:process') {
      this.queueEmitter.enqueue(msg.job);
      return;
    }

    // unknown/misrouted message — don't silently swallow it
    console.warn(`[${this.name}] ignoring unrecognized message type "${msg.type}"`);
  }

  _onResult({ ok, job, result, error, trace }) {
    const outgoingTrace = {
      ...trace,
      childName: this.name,
      sentAt: Date.now(),
    };

    // always send a copy to the tracer child, job.id preserved for correlation
    this.send({
      type: 'job:forward',
      from: this.name,
      to: 'tracer',
      job,
      data: outgoingTrace,
      meta: { kind: 'trace' },
    });

    if (ok && this.forwardTo.length) {
      for (const target of this.forwardTo) {
        this.send({
          type: 'job:forward',
          from: this.name,
          to: target,
          job, // same job.id, forwarded onward for further processing
          data: result,
          meta: { kind: 'data' },
        });
      }
      return;
    }

    this.send({
      type: 'job:result',
      from: this.name,
      ok,
      job,
      data: result,
      error,
      trace: outgoingTrace,
    });
  }

  async shutdown(signal) {
    this.queueEmitter.stop();
    await this.worker.terminate();
    process.exit(0);
  }
}

module.exports = ChildProcess;