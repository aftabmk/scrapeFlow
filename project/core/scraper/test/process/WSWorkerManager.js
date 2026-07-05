const { BaseWorkerManager } = require('./BaseWorkerManager');

class WSWorkerManager extends BaseWorkerManager {
  static key = 'wsChild';
  static readyType = 'ready';
  static reuseWarm = true;

  static async run(workerPath) {
    const { child } = await this.ensure(workerPath, { cmd: 'start', port: 8080 });
    return child;
  }
}

module.exports = { WSWorkerManager };