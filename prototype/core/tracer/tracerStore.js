// TracerStore.js

const { Tracer } = require('./models/tracer');

class TracerStore {
  static #map = new Map();
  
  #save() {
    const id = this.tracer.jobId;
    if (!TracerStore.#map.has(id)) {
      TracerStore.#map.set(id, []);
    }

    TracerStore.#map.get(id).push(
      Object.entries(this.tracer).reduce((acc, [key, value]) => {
        if (value !== undefined) acc[key] = value;
        return acc;
      }, {})
    );
  }

  constructor(payload = {}) {
    this.tracer = new Tracer(
      payload.jobId,
      payload.class,
      payload.function,
      payload.status,
      payload.message
    );
    this.#save();
  }

  static traceWithId(jobId) {
    return TracerStore.#map.get(jobId) ?? [];
  }

  static traceAll() {
    return [...TracerStore.#map.entries()].reduce((acc, [jobId, traces]) => {
      acc[jobId] = traces;
      return acc;
    }, {});
  }
}

module.exports = { TracerStore };