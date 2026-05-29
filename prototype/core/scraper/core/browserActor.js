const JobEvent = require('../../../events/jobEvent');

class BrowserActor {
  constructor() {
    this._healthTimer = null;
  }

  async init() {
    throw new Error(`[${this.constructor.name}] init() must be implemented`);
  }

  async close() {
    throw new Error(`[${this.constructor.name}] close() must be implemented`);
  }

  subscribeToJobs() {
    JobEvent.subscribe(async (job) => {
      console.log(`[${this.constructor.name}] job received:`, job);
      await this.onJob(job);
    });
  }

  async onJob(job) {
    throw new Error(`[${this.constructor.name}] onJob() must be implemented`);
  }

  async healthCheck() {
    throw new Error(`[${this.constructor.name}] healthCheck() must be implemented`);
  }

  _startHealthCheck(intervalMs = 30_000) {
    this._healthTimer = setInterval(async () => {
      await this.healthCheck();
    }, intervalMs);
  }

  _stopHealthCheck() {
    if (this._healthTimer) {
      clearInterval(this._healthTimer);
      this._healthTimer = null;
    }
  }
}

module.exports = BrowserActor;