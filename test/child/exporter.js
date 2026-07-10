// child/exporter.js
const BaseChildProcess = require('./base');

class ExporterChildProcess extends BaseChildProcess {
  constructor(options = {}) {
    super({
      ...options,
      processType: 'exporter',
      queueName: options.queueName || 'export_queue'
    });
  }

  _getTaskHandler() {
    return async (job) => {
      const { data, target } = job.data || {};
      
      console.log(`[Exporter] Exporting to ${target || 'external DB'}...`);
      await this._sleep(1000);
      
      return {
        exported: true,
        target: target || 'postgresql',
        recordId: `exp_${Date.now()}`,
        exportedAt: new Date().toISOString()
      };
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = ExporterChildProcess;