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

  async _processJob(job) {
    const data = job.data || {};
    
    console.log(`[Exporter] 📤 Received export job: ${job.job_id}`);
    console.log(`[Exporter] 📊 EXCHANGE: ${data.exchange || 'N/A'}`);
    console.log(`[Exporter] 📊 CONTRACT: ${data.contract || 'N/A'}`);
    console.log(`[Exporter] 📊 PAGE_URL: ${data.pageUrl || 'N/A'}`);
    console.log(`[Exporter] 📊 API_URL: ${data.apiUrl || 'N/A'}`);
    console.log(`[Exporter] 📊 REFERER: ${data.referer || 'N/A'}`);
    
    // Simulate export work
    const delay = 500 + Math.random() * 1000;
    await this._sleep(delay);
    
    return {
      jobId: job.job_id,
      event: data.event || {},
      exported: true,
      exportedAt: new Date().toISOString(),
      processingTime: delay,
      exportResult: {
        status: 'logged_only',
        message: 'Exporter processed event - no actual export performed'
      }
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  new ExporterChildProcess();
}

module.exports = ExporterChildProcess;