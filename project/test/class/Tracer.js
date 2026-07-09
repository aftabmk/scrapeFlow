'use strict';

const Tracer = require('../core/Tracer');

class TracerHandler {
  constructor(_options = {}) {
    this.tracer = new Tracer();
  }

  async run(job) {
    const { id, args } = job;
    const [traceEntry] = args;

    this.tracer.append(traceEntry.childName || 'unknown', {
      ...traceEntry,
      jobId: id,
    });

    return { stored: true, jobId: id };
  }
}

module.exports = TracerHandler;