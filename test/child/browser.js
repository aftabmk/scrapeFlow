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

  _getTaskHandler() {
    return async (job) => {
      const { url, selector } = job.data;
      
      console.log(`[Browser] Scraping: ${url}`);
      await this._sleep(2000);
      
      return {
        url,
        selector,
        data: [
          { text: 'Sample data 1', timestamp: Date.now() },
          { text: 'Sample data 2', timestamp: Date.now() }
        ],
        scrapedAt: new Date().toISOString()
      };
    };
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = BrowserChildProcess;