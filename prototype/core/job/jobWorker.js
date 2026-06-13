// events
const JobEvent       = require('../../events/jobEvent');
const TracerEvent    = require('../../events/tracerEvent');

const { JobBuilder } = require('./models/JobBuilder');

class JobWorker {
  constructor(events) {
    this._builder = new JobBuilder(events);
  }
  // private

  #traceOnFail() {
    TracerEvent.trace({ jobId: null, class: 'JobWorker', function: 'run', status: 'failure', message: 'No valid jobs found' });
    console.warn('[JobWorker] No valid jobs found — nothing emitted.');
  }
  
  #traceOnSuccess(job) {
    TracerEvent.trace({ jobId: job.id, class: 'JobWorker', function: 'run', status: 'success' });
  }

  // public
  run() {
    const jobs = this._builder.buildAll();

    if (jobs.length === 0) {
      this.#traceOnFail();
      return;
    }
    
    for (const job of jobs) {
      JobEvent.emit(job);
      this.#traceOnSuccess(job);
      console.log({"[jobWorker] decode job ":job.decode()});
    }
    
    console.log(`[JobWorker] Emitted ${jobs.length} job(s).`);
  }
  
}

module.exports = { JobWorker };