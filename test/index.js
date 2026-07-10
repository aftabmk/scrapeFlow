// index.js
const Orchestrator = require('./parent/orchestrator');

class Application {
  constructor() {
    this.orchestrator = null;
  }

  async start() {
    console.log('🚀 Starting Application...');

    try {
      this.orchestrator = new Orchestrator({
        heartbeatTimeout: 10000,
        restartDelay: 2000
      });

      console.log('📦 Starting SQLite Server...');
      await this.orchestrator.startSQLiteServer({
        dbPath: './data/queue.db',
        readWorkers: 3
      });

      this._setupEventListeners();

      console.log('📦 Creating child processes...');

      // Create Browser Process
      await this.orchestrator.createProcess({
        type: 'browser',
        processingWorkers: 4,
        commWorkers: 1,
        queueName: 'browser_queue'
      });

      // Create Analyzer Process
      await this.orchestrator.createProcess({
        type: 'analyzer',
        processingWorkers: 3,
        commWorkers: 1,
        queueName: 'analyzer_queue'
      });

      // Create Exporter Process
      await this.orchestrator.createProcess({
        type: 'exporter',
        processingWorkers: 2,
        commWorkers: 1,
        queueName: 'export_queue'
      });

      console.log('✅ All processes started!');

      await this._submitTestJobs();

      console.log('🎯 System ready!');
      console.log('📊 Press Ctrl+C to stop');
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
      console.log(`⚠️ Process ${pid} (${type}) exited with code ${code}`);
    });

    this.orchestrator.on('heartbeatTimeout', ({ pid }) => {
      console.log(`⏰ Heartbeat timeout for process ${pid}`);
    });

    this.orchestrator.on('jobQueued', ({ pid, jobId }) => {
      console.log(`📝 Job ${jobId} queued in process ${pid}`);
    });

    this.orchestrator.on('jobError', ({ pid, jobId, error }) => {
      console.log(`❌ Job ${jobId} error:`, error);
    });
  }

  async _submitTestJobs() {
    console.log('\n📤 Submitting test jobs...');

    // Submit Browser jobs
    for (let i = 0; i < 5; i++) {
      const job = {
        type: 'browser',
        data: {
          url: `https://example.com/page/${i}`,
          selector: `.content-${i}`,
          options: { timeout: 30000 }
        }
      };

      try {
        const result = await this.orchestrator.submitJob(job);
        console.log(`✅ Browser job ${result.jobId} submitted`);
      } catch (error) {
        console.error(`❌ Browser job submission failed:`, error.message);
      }
    }

    // Submit Analyzer jobs
    for (let i = 0; i < 3; i++) {
      const job = {
        type: 'analyzer',
        data: {
          data: { id: i, content: `Sample data ${i}` },
          options: { deep: true }
        }
      };

      try {
        const result = await this.orchestrator.submitJob(job);
        console.log(`✅ Analyzer job ${result.jobId} submitted`);
      } catch (error) {
        console.error(`❌ Analyzer job failed:`, error.message);
      }
    }

    // Submit Exporter jobs
    for (let i = 0; i < 2; i++) {
      const job = {
        type: 'exporter',
        data: {
          data: { id: i, content: `Export data ${i}` },
          target: 'postgresql'
        }
      };

      try {
        const result = await this.orchestrator.submitJob(job);
        console.log(`✅ Exporter job ${result.jobId} submitted`);
      } catch (error) {
        console.error(`❌ Exporter job failed:`, error.message);
      }
    }

    console.log('\n📊 All test jobs submitted!');
  }

  async stop() {
    console.log('\n🛑 Shutting down...');
    if (this.orchestrator) {
      await this.orchestrator.shutdown();
    }
    console.log('✅ Shutdown complete');
    process.exit(0);
  }
}

// === Graceful Shutdown ===

const app = new Application();

process.on('SIGINT', async () => {
  console.log('\nReceived SIGINT');
  await app.stop();
});

process.on('SIGTERM', async () => {
  console.log('\nReceived SIGTERM');
  await app.stop();
});

// Start application with error handling
app.start().catch((error) => {
  console.error('❌ Application failed:', error);
  process.exit(1);
});

module.exports = app;