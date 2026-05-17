class Tracer {
  constructor(payload = {}) {
    this.jobId = payload.jobId;
    if (payload.class    !== undefined) this.class    = payload.class;
    if (payload.function !== undefined) this.function = payload.function;
    if (payload.status   !== undefined) this.status   = payload.status;
    if (payload.message  !== undefined) this.message  = payload.message;
  }

  getData() {
    const data = { jobId: this.jobId };
    if (this.class    !== undefined) data.class    = this.class;
    if (this.function !== undefined) data.function = this.function;
    if (this.status   !== undefined) data.status   = this.status;
    if (this.message  !== undefined) data.message  = this.message;
    return data;
  }
}

module.exports = { Tracer };