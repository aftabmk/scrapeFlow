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
        
        console.log(`[Browser] 🌐 Scraping: ${data.exchange || 'N/A'} - ${data.contract || 'N/A'}`);
        console.log(`[Browser] 📋 Job ID: ${job.id}`);
        console.log(`[Browser] 📋 URL: ${data.pageUrl || 'N/A'}`);
        
        await this._sleep(1000 + Math.random() * 1500);
        
        return {
            jobId: job.id,
            event: data.event || {},
            exchange: data.exchange,
            contract: data.contract,
            pageUrl: data.pageUrl,
            apiUrl: data.apiUrl,
            referer: data.referer,
            browserProcessed: true,
            scrapedAt: new Date().toISOString()
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