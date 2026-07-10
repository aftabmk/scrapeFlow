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

  _getTaskHandler() {
    return async (job) => {
      const { data } = job.data || {};
      
      console.log(`[Analyzer] Analyzing data...`);
      await this._sleep(1500);
      
      return {
        original: data,
        analyzed: true,
        category: this._categorize(data),
        confidence: Math.random() * 0.5 + 0.5,
        analyzedAt: new Date().toISOString()
      };
    };
  }

  _categorize(data) {
    const categories = ['technology', 'business', 'science', 'health'];
    return categories[Math.floor(Math.random() * categories.length)];
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = AnalyzerChildProcess;