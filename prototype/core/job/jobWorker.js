// JobWorker.js
// Lifecycle: build → emit
// Validation is handled inside Job constructor.

const { JobBuilder } = require('./models/JobBuilder');
const JobEvent       = require('../../events/jobEvent');
const TracerEvent    = require('../../events/tracerEvent');

class JobWorker {
  constructor() {
    this._builder = new JobBuilder();
  }

  run() {
    const jobs = this._builder.buildAll();  // already validated Job[]

    if (jobs.length === 0) {
      console.warn('[JobWorker] No valid jobs found — nothing emitted.');
      return;
    }

    for (const job of jobs) {
      const key = `job:${job.exchange}:${job.createdAt}`;
      TracerEvent.trace(key, ['JobWorker', 'run', 'JobEvent']);
      JobEvent.emit(job);
    }

    console.log(`[JobWorker] Emitted ${jobs.length} job(s).`);
  }
}

module.exports = { JobWorker };