// main.js
const Orchestrator = require('./parent/orchestrator');

class Application {
  constructor(config = {}) {
    this.config = config;
    this.orchestrator = null;
    this.jobSubmitterStarted = false;
    this.events = config.events || [];
  }

  async start() {
    const { app: appConfig, workers, events } = this.config;
    
    console.log(`🚀 Starting Application (${this.config.env})...`);
    console.log(`📋 Loaded ${events.length} events from config`);

    try {
      this.orchestrator = new Orchestrator({
        heartbeatTimeout: appConfig.heartbeatTimeout,
        restartDelay: appConfig.restartDelay
      });

      this._setupEventListeners();

      console.log('📦 Creating child processes...');

      await this.orchestrator.createProcess({
        type: 'analyzer',
        processingWorkers: workers.analyzer,
        queueName: 'analyzer_queue'
      });

      await this.orchestrator.createProcess({
        type: 'browser',
        processingWorkers: workers.browser,
        queueName: 'browser_queue'
      });

      await this.orchestrator.createProcess({
        type: 'exporter',
        processingWorkers: workers.exporter,
        queueName: 'export_queue'
      });

      await this.orchestrator.createProcess({
        type: 'job-submitter',
        processingWorkers: 0,
        queueName: 'job_submitter_queue'
      });

      console.log('✅ All processes created!');

      this.orchestrator.once('allProcessesReady', async () => {
        if (!this.jobSubmitterStarted) {
          this.jobSubmitterStarted = true;
          console.log('\n🎯 All processes ready! Starting job submitter...\n');
          await this.orchestrator.startJobSubmitter({
            events: events,
            maxJobs: appConfig.maxJobs || events.length,
            submitInterval: appConfig.submitInterval
          });
        }
      });

      console.log('🎯 System ready!');
      console.log('📊 Press Ctrl+C to stop');
      
      return this;

    } catch (error) {
      console.error('❌ Failed to start application:', error);
      throw error;
    }
  }

  _setupEventListeners() {
    this.orchestrator.on('processReady', ({ pid, type, processingWorkers }) => {
      console.log(`✅ Process ${pid} (${type}) ready with ${processingWorkers} workers`);
    });

    this.orchestrator.on('processExit', ({ pid, type, code }) => {
      if (code !== 0) {
        console.log(`⚠️ Process ${pid} (${type}) exited with code ${code}`);
      }
    });

    this.orchestrator.on('jobFullyComplete', ({ jobId }) => {
      console.log(`🎉 Job ${jobId} fully completed through all stages!`);
    });

    this.orchestrator.on('submitterStarted', ({ maxJobs, submitInterval }) => {
      console.log(`📤 Job submitter started: ${maxJobs} jobs, interval: ${submitInterval}ms`);
    });

    this.orchestrator.on('submitterComplete', ({ totalJobs }) => {
      console.log(`✅ All ${totalJobs} events processed by job-submitter!`);
    });

    this.orchestrator.on('jobSubmitted', ({ jobNumber, totalJobs, jobId, eventData }) => {
      if (eventData) {
        console.log(`📤 Event ${jobNumber}/${totalJobs}: ${eventData.EXCHANGE} - ${eventData.CONTRACT}`);
      }
    });

    this.orchestrator.on('jobSubmissionError', ({ jobNumber, error }) => {
      console.error(`❌ Event ${jobNumber} submission failed:`, error);
    });

    this.orchestrator.on('heartbeatTimeout', ({ pid }) => {
      console.log(`⏰ Heartbeat timeout for process ${pid}`);
    });
  }

  async stop() {
    console.log('\n🛑 Shutting down...');
    if (this.orchestrator) {
      await this.orchestrator.shutdown();
    }
    console.log('✅ Shutdown complete');
  }

  getOrchestrator() {
    return this.orchestrator;
  }

  getProcessStats() {
    return this.orchestrator ? this.orchestrator.getProcessStats() : null;
  }

  async submitJob(jobData) {
    if (!this.orchestrator) {
      throw new Error('Orchestrator not initialized');
    }
    return this.orchestrator.submitJob(jobData);
  }
}

module.exports = Application;