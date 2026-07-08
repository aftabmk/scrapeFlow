'use strict';

const EventEmitter = require('events');

class DurableQueue {
  constructor() {
    this._items = [];
  }
  enqueue(item) {
    this._items.push(item);
  }
  dequeue(n = 1) {
    return this._items.splice(0, n);
  }
  get size() {
    return this._items.length;
  }
}

class QueueEmitter extends EventEmitter {
  constructor(workerHandle, opts = {}) {
    super();
    this.worker = workerHandle;
    this.queue = new DurableQueue();
    this.concurrency = opts.concurrency || 1;
    this.inFlight = 0;
    this.running = false;

    this.worker.on('message', (msg) => this._onWorkerMessage(msg));

    this.on('task', () => this._pump());
  }

  start() {
    this.running = true;
    this._pump();
  }

  stop() {
    this.running = false;
  }

  enqueue(job, meta = {}) {
    this.queue.enqueue({ job, meta, traceId: meta.traceId, enteredAt: Date.now() });
    this.emit('task');
  }

  _pump() {
    if (!this.running) return;

    const freeSlots = this.concurrency - this.inFlight;
    if (freeSlots <= 0 || this.queue.size === 0) return;

    const batch = this.queue.dequeue(freeSlots);
    for (const item of batch) {
      this.inFlight += 1;
      this.worker.postMessage({ traceId: item.traceId, job: item.job });
      this._pending = this._pending || new Map();
      this._pending.set(item.traceId, item);
    }
  }

  _onWorkerMessage(msg) {
    this.inFlight = Math.max(0, this.inFlight - 1);

    const pendingItem = this._pending && this._pending.get(msg.trace?.traceId);
    this._pending && this._pending.delete(msg.trace?.traceId);

    this._ack(msg, pendingItem);
    this._pump();
  }

  _ack(msg, pendingItem) {
    const enteredAt = pendingItem ? pendingItem.enteredAt : undefined;

    this.emit('result', {
      ok: msg.ok,
      job: msg.job,
      result: msg.result,
      error: msg.error,
      trace: {
        ...msg.trace,
        enteredAt,
      },
    });

    this.emit('task');
  }
}

module.exports = { QueueEmitter, DurableQueue };