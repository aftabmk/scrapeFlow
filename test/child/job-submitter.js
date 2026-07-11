// child/job-submitter.js
const BaseChildProcess = require('./base');

class JobSubmitterProcess extends BaseChildProcess {
  constructor(options = {}) {
    super({
      ...options,
      processType: 'job-submitter',
      queueName: options.queueName || 'job_submitter_queue',
      processingWorkers: 0
    });
    
    this.jobsSubmitted = 0;
    this.maxJobs = 10;
    this.submitInterval = 3000;
    this.isSubmitting = false;
    this.submitTimer = null;
    this.events = [];
    this.currentEventIndex = 0;
    
    console.log(`[JobSubmitter] Waiting for start signal from orchestrator...`);
  }

  _setupIPCListener() {
    process.on('message', async (message) => {
      if (!message || !message.type) return;

      switch (message.type) {
        case 'START_SUBMITTING':
          console.log(`[JobSubmitter] 🚀 Received START_SUBMITTING signal`);
          console.log(`[JobSubmitter] 📋 Received ${message.config?.events?.length || 0} events to process`);
          await this._startSubmitting(message.config || {});
          break;
        case 'SHUTDOWN':
          await this.shutdown();
          break;
        case 'GET_STATUS':
          this._sendStatus();
          break;
        default:
          console.log(`[JobSubmitter] Received: ${message.type}`);
      }
    });
  }

  async _startSubmitting(config = {}) {
    if (this.isSubmitting) return;
    this.isSubmitting = true;
    
    this.events = config.events || [];
    this.maxJobs = config.maxJobs || this.events.length || 10;
    this.submitInterval = config.submitInterval || 3000;
    this.jobsSubmitted = 0;
    this.currentEventIndex = 0;
    
    console.log(`[JobSubmitter] 📤 Starting to submit ${this.maxJobs} events...`);
    console.log(`[JobSubmitter] ⏱️ Submit interval: ${this.submitInterval}ms`);
    
    process.send({
      type: 'SUBMITTER_STARTED',
      maxJobs: this.maxJobs,
      submitInterval: this.submitInterval,
      timestamp: Date.now()
    });

    // Submit first event immediately
    await this._submitNextEvent();
    
    // Start interval for subsequent events
    this.submitTimer = setInterval(async () => {
      if (this.jobsSubmitted >= this.maxJobs || !this.isRunning) {
        clearInterval(this.submitTimer);
        this.submitTimer = null;
        this.isSubmitting = false;
        console.log(`[JobSubmitter] ✅ All ${this.maxJobs} events submitted!`);
        process.send({
          type: 'SUBMITTER_COMPLETE',
          totalJobs: this.jobsSubmitted,
          timestamp: Date.now()
        });
        return;
      }
      
      await this._submitNextEvent();
    }, this.submitInterval);
  }

  async _submitNextEvent() {
    if (this.jobsSubmitted >= this.maxJobs) return;
    if (this.currentEventIndex >= this.events.length) return;
    
    const eventData = this.events[this.currentEventIndex];
    const jobNumber = this.jobsSubmitted + 1;
    
    console.log(`[JobSubmitter] 📤 Submitting event ${jobNumber}/${this.maxJobs}:`);
    console.log(`[JobSubmitter]   EXCHANGE: ${eventData.EXCHANGE}`);
    console.log(`[JobSubmitter]   CONTRACT: ${eventData.CONTRACT}`);
    console.log(`[JobSubmitter]   PAGE_URL: ${eventData.PAGE_URL}`);
    
    // Create job from event
    const job = {
      type: 'analyzer', // All events start as analyzer jobs
      data: {
        event: eventData,
        exchange: eventData.EXCHANGE,
        contract: eventData.CONTRACT,
        pageUrl: eventData.PAGE_URL,
        apiUrl: eventData.API_URL,
        apiUrlBuilder: eventData.API_URL_BUILDER || null,
        referer: eventData.REFERER,
        metadata: {
          source: 'job-submitter',
          submittedAt: new Date().toISOString(),
          eventIndex: this.currentEventIndex,
          batchId: `batch_${Date.now()}`
        }
      }
    };
    
    process.send({
      type: 'SUBMIT_JOB',
      job,
      jobNumber: jobNumber,
      totalJobs: this.maxJobs,
      eventData: eventData,
      timestamp: Date.now()
    });
    
    this.jobsSubmitted++;
    this.currentEventIndex++;
  }

  _createJob(jobNumber) {
    // This is a fallback - actual events come from the config
    return {
      type: 'analyzer',
      data: {
        url: `https://example.com/page/${jobNumber}`,
        selectors: {
          title: `h1.title-${jobNumber}`,
          content: `.content-${jobNumber}`
        },
        options: {
          waitFor: 'networkidle',
          timeout: 30000,
          maxRetries: 3
        },
        priority: 5,
        metadata: {
          source: 'job-submitter',
          submittedAt: new Date().toISOString(),
          jobNumber: jobNumber
        }
      }
    };
  }

  _sendStatus() {
    process.send({
      type: 'STATUS',
      processType: this.processType,
      jobsSubmitted: this.jobsSubmitted,
      maxJobs: this.maxJobs,
      isSubmitting: this.isSubmitting,
      eventsRemaining: this.events.length - this.currentEventIndex,
      pid: process.pid
    });
  }

  async shutdown() {
    console.log(`[JobSubmitter] Shutting down...`);
    this.isRunning = false;
    this.isSubmitting = false;
    
    if (this.submitTimer) {
      clearInterval(this.submitTimer);
      this.submitTimer = null;
    }
    
    process.send({ type: 'SHUTDOWN_COMPLETE' });
    setTimeout(() => process.exit(0), 500);
  }
}

if (require.main === module) {
  new JobSubmitterProcess();
}

module.exports = JobSubmitterProcess;