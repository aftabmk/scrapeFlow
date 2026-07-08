'use strict';

const path = require('path');

const { Worker } = require('worker_threads');
const ProcessBase = require('./ProcessBase');
const { QueueEmitter } = require('./QueueEmitter');

class ChildProcess extends ProcessBase {
  constructor(opts) {
    super({ name: opts.name });
    this.handlerPath = opts.handlerPath;

    this.worker = new Worker(path.join(__dirname, 'WorkerThreadWrapper.js'), {
      workerData: { handlerPath: this.handlerPath, handlerOptions: opts.handlerOptions },
    });

    this.queueEmitter = new QueueEmitter(this.worker, { concurrency: opts.concurrency });
    this.queueEmitter.on('result', (payload) => this._onResult(payload));

    this.queueEmitter.start();
    this.signalReady();
  }

  onMessage(msg) {
    if (msg.type === 'ready' || msg.type === 'shutdown') return; // control messages
    this.queueEmitter.enqueue(msg.job, { traceId: msg.traceId });
  }

  _onResult({ ok, job, result, error, trace }) {
    const outgoingTrace = {
      ...trace,
      childName: this.name,
      sentAt: Date.now(),
    };

    this.tracer.append(this.name, outgoingTrace);

    const shouldForward = true;
    if (shouldForward) {
      this.send({
        type: 'result',
        from: this.name,
        ok,
        job,
        data: result,
        error,
        trace: outgoingTrace,
      });
    }
  }

  async shutdown(signal) {
    this.queueEmitter.stop();
    await this.worker.terminate();
    process.exit(0);
  }
}

module.exports = ChildProcess;