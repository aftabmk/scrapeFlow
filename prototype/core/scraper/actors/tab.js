const TabActor = require('../core/tabActor');

class Tab extends TabActor {
  constructor(page, onClose) {
    super();
    this.page    = page;
    this.onClose = onClose;
  }

  async init() {
    this._startHealthCheck();
  }

  async processJob(job) {
    this.job = job;
    await this.pageVisit(job.page_url);
    await this.fetch(job.api_url);
  }

  async healthCheck() {
    try {
      if (this.page.isClosed()) {
        console.warn('[Tab] page found closed during health check');
        await this.close();
        return;
      }
      await this.page.evaluate(() => true);
    } catch {
      console.warn('[Tab] page unresponsive — closing');
      await this.close();
    }
  }

  async close() {
    this._stopHealthCheck();
    this.session = null;
    try {
      if (!this.page.isClosed()) await this.page.close();
    } catch { /* already closed */ }
    this.onClose();
  }
}

module.exports = Tab;