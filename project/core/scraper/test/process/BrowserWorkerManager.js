const { BaseWorkerManager } = require('./BaseWorkerManager');

class BrowserWorkerManager extends BaseWorkerManager {
  static key = 'browserChild';
  static readyType = 'done';
  static reuseWarm = false; // re-invoke scrape each call on the same warm process

  static async run(workerPath) {
    const { data } = await this.ensure(workerPath, { cmd: 'scrape' });
    return data;
  }
}

module.exports = { BrowserWorkerManager };