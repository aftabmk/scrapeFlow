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
        
        console.log(`[Analyzer] 🔍 Analyzing: ${event.EXCHANGE || 'N/A'} - ${event.CONTRACT || 'N/A'}`);
        console.log(`[Analyzer] 📋 Job ID: ${job.id}`);
        console.log(`[Analyzer] 📋 URL: ${event.PAGE_URL || 'N/A'}`);
        
        // Simulate analysis work
        await this._sleep(500 + Math.random() * 1000);
        
        return {
            jobId: job.id,
            event: event,
            analyzed: true,
            analyzedAt: new Date().toISOString(),
            exchange: event.EXCHANGE,
            contract: event.CONTRACT,
            pageUrl: event.PAGE_URL,
            apiUrl: event.API_URL,
            apiUrlBuilder: event.API_URL_BUILDER || null,
            referer: event.REFERER,
            metadata: data.metadata || {}
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