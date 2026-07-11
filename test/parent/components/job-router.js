// parent/components/job-router.js
const { EventEmitter } = require('events');

class JobRouter extends EventEmitter {
  constructor(processManager) {
    super();
    this.processManager = processManager;
    this.pendingSubmissions = [];
    this.isProcessing = false;
    this.submitJobFn = null;
  }

  setSubmitJobFn(fn) {
    this.submitJobFn = fn;
  }

  async routeToBrowser(message) {
    const result = message.result;
    
    const browserProcess = await this.processManager.waitForProcess('browser');
    if (!browserProcess) {
      console.error('[JobRouter] ❌ Browser not available');
      return;
    }
    
    console.log(`[JobRouter] 🔄 Routing to browser: ${message.jobId}`);
    
    const browserJob = {
      type: 'browser',
      data: {
        event: result.event || {},
        exchange: result.exchange,
        contract: result.contract,
        pageUrl: result.pageUrl,
        apiUrl: result.apiUrl,
        apiUrlBuilder: result.apiUrlBuilder,
        referer: result.referer,
        analysisJobId: message.jobId,
        analyzedAt: result.analyzedAt
      }
    };
    
    try {
      await this.submitJobFn(browserJob);
      console.log(`[JobRouter] ✅ Browser job created`);
    } catch (error) {
      console.error(`[JobRouter] ❌ Failed to route to browser:`, error.message);
    }
  }

  async routeToExporter(message) {
    const result = message.result;
    
    const exporterProcess = await this.processManager.waitForProcess('exporter');
    if (!exporterProcess) {
      console.error('[JobRouter] ❌ Exporter not available');
      return;
    }
    
    console.log(`[JobRouter] 🔄 Routing to exporter: ${message.jobId}`);
    
    const exporterJob = {
      type: 'exporter',
      data: {
        event: result.event || {},
        exchange: result.exchange,
        contract: result.contract,
        pageUrl: result.pageUrl,
        apiUrl: result.apiUrl,
        referer: result.referer,
        browserJobId: message.jobId,
        scrapedAt: result.scrapedAt || new Date().toISOString()
      }
    };
    
    try {
      await this.submitJobFn(exporterJob);
      console.log(`[JobRouter] ✅ Exporter job created`);
    } catch (error) {
      console.error(`[JobRouter] ❌ Failed to route to exporter:`, error.message);
    }
  }

  async handleJobSubmission(message) {
    const { job, jobNumber, totalJobs, eventData } = message;
    
    if (this.isProcessing) {
      this.pendingSubmissions.push(message);
      return;
    }
    
    this.isProcessing = true;
    
    try {
      const analyzerProcess = await this.processManager.waitForProcess('analyzer');
      if (!analyzerProcess) {
        console.error(`[JobRouter] ❌ Analyzer not available for job ${jobNumber}`);
        this.emit('jobError', { jobNumber, error: 'Analyzer not available' });
        return;
      }
      
      console.log(`[JobRouter] 📤 Submitting job ${jobNumber}/${totalJobs}`);
      if (eventData) {
        console.log(`[JobRouter] 📋 ${eventData.EXCHANGE} - ${eventData.CONTRACT}`);
      }
      
      const result = await this.submitJobFn(job);
      this.emit('jobSubmitted', { jobNumber, totalJobs, jobId: result.jobId, eventData });
      
    } catch (error) {
      console.error(`[JobRouter] ❌ Job ${jobNumber} failed:`, error.message);
      this.emit('jobError', { jobNumber, error: error.message });
    } finally {
      this.isProcessing = false;
      this._processNext();
    }
  }

  _processNext() {
    if (this.pendingSubmissions.length > 0 && !this.isProcessing) {
      const next = this.pendingSubmissions.shift();
      this.handleJobSubmission(next);
    }
  }
}

module.exports = JobRouter;