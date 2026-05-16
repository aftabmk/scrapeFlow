const JobEvent    = require('../../events/jobEvent');
const TracerEvent = require('../../events/tracerEvent');

class JobClass {
  constructor() {}

  createJob(payload) {
    const key = `${payload.type}:${payload.symbol}:${Date.now()}`;
    const job = { ...payload, key, createdAt: Date.now() };
    TracerEvent.trace(key, ['JobClass', 'createJob', 'JobEvent']);
    JobEvent.emit(job);
    return job;
  }

  run(payload) {
    this.createJob(payload);
  }
}

module.exports = JobClass;