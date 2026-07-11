// child/browser.js
const BaseChildProcess = require('./base');

class BrowserChildProcess extends BaseChildProcess {
  constructor(options = {}) {
    super({
      ...options,
      processType: 'browser',
      queueName: options.queueName || 'browser_queue'
    });
  }

  async _processJob(job) {
    const data = job.data || {};
    
    console.log(`[Browser] 🌐 Received browser job: ${job.job_id}`);
    console.log(`[Browser] 📋 EXCHANGE: ${data.exchange || 'N/A'}`);
    console.log(`[Browser] 📋 CONTRACT: ${data.contract || 'N/A'}`);
    console.log(`[Browser] 📋 PAGE_URL: ${data.pageUrl || 'N/A'}`);
    console.log(`[Browser] 📋 API_URL: ${data.apiUrl || 'N/A'}`);
    console.log(`[Browser] 📋 REFERER: ${data.referer || 'N/A'}`);
    
    // Simulate browser scraping
    const delay = 1000 + Math.random() * 1500;
    await this._sleep(delay);
    
    return {
      jobId: job.job_id,
      event: data.event || {},
      browserProcessed: true,
      scrapedAt: new Date().toISOString(),
      processingTime: delay,
      // Forward to exporter
      exchange: data.exchange,
      contract: data.contract,
      pageUrl: data.pageUrl,
      apiUrl: data.apiUrl,
      referer: data.referer
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

if (require.main === module) {
  new BrowserChildProcess();
}

module.exports = BrowserChildProcess;