// child/analyzer.js
const BaseChildProcess = require('./base');

class AnalyzerChildProcess extends BaseChildProcess {
  constructor(options = {}) {
    super({
      ...options,
      processType: 'analyzer',
      queueName: options.queueName || 'analyzer_queue'
    });
  }

  async _processJob(job) {
    const data = job.data || {};
    const event = data.event || {};
    
    console.log(`[Analyzer] 🔍 Analyzing event for job: ${job.job_id}`);
    console.log(`[Analyzer] 📊 EXCHANGE: ${event.EXCHANGE || 'N/A'}`);
    console.log(`[Analyzer] 📊 CONTRACT: ${event.CONTRACT || 'N/A'}`);
    console.log(`[Analyzer] 📊 PAGE_URL: ${event.PAGE_URL || 'N/A'}`);
    console.log(`[Analyzer] 📊 API_URL: ${event.API_URL || 'N/A'}`);
    console.log(`[Analyzer] 📊 REFERER: ${event.REFERER || 'N/A'}`);
    
    // Simulate analysis work
    const delay = 500 + Math.random() * 1000;
    await this._sleep(delay);
    
    // Pass through the event data with analysis metadata
    return {
      jobId: job.job_id,
      event: event,
      analyzed: true,
      analyzedAt: new Date().toISOString(),
      processingTime: delay,
      // Forward to browser
      pageUrl: event.PAGE_URL,
      apiUrl: event.API_URL,
      apiUrlBuilder: event.API_URL_BUILDER,
      referer: event.REFERER,
      exchange: event.EXCHANGE,
      contract: event.CONTRACT
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  new AnalyzerChildProcess();
}

module.exports = AnalyzerChildProcess;