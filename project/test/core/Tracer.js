'use strict';

class Tracer {
  constructor() {
    this.store = new Map();
  }

  append(childName, entry) {
    if (!this.store.has(childName)) {
      this.store.set(childName, []);
    }
    this.store.get(childName).push({
      ...entry,
      recordedAt: Date.now(),
    });
  }

  getByChild(childName) {
    return this.store.get(childName) || [];
  }

  getByJobId(jobId) {
    const results = [];
    for (const [childName, entries] of this.store.entries()) {
      for (const entry of entries) {
        if (entry.jobId === jobId) {
          results.push({ childName, ...entry });
        }
      }
    }
    return results.sort((a, b) => (a.recordedAt - b.recordedAt));
  }

  getAll() {
    return this.store;
  }
}

module.exports = Tracer;