class Tracer {
  constructor(jobId, cls, fn, status, message) {
    this.jobId    = jobId;
    this.class    = cls;
    this.function = fn;
    this.status   = status;
    this.message  = message;
  }
}

module.exports = { Tracer };