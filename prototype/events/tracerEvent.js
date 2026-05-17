// TracerEvent.js

const eventBus   = require('../eventBus');
const { Tracer } = require('../core/tracer/tracer');

class TracerEvent {
  constructor() {
    this.TRACE   = 'tracer:trace';
    this._store  = new Map();  // jobId → Tracer[]
  }

  trace(payload) {
    const tracer = new Tracer(payload);
    const data   = tracer.getData();

    // Store in hashmap grouped by jobId
    if (!this._store.has(payload.jobId)) {
      this._store.set(payload.jobId, []);
    }
    this._store.get(payload.jobId).push(data);

    eventBus.emit(this.TRACE, data);
  }

  traceWithId(jobId) {
    return this._store.get(jobId) ?? [];
  }

  subscribe(onTrace) {
    eventBus.on(this.TRACE, onTrace);
  }
}

module.exports = new TracerEvent();