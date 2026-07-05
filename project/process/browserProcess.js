// browserProcess.js
const JobEvent = require('../events/jobEvent');

const { Job } = require('../core/job/models/job');
const Browser = require('../core/scraper/core/browser/browser');

const browserProcess = async () => {
  try {
    const browser = Browser.getInstance();
    await browser.init();

    process.send({ type: 'ready' });

    process.on('message', async (msg) => {
      switch (msg.type) {
        case 'trigger-poll':
          process.send({ type: 'dequeue-request', batchSize: 5, requestId: Date.now() });
          break;

        case 'dequeue-response': {
          // job should converted to class as browser need job class instead of job variable
          const jobs = msg.jobs.map(raw => Object.assign(Object.create(Job.prototype), raw));

          for (const job of jobs) {
            try {
              // const decoded = job.decode();
              // Emit jobs as browser will listen to job using event emitter
              JobEvent.emit(job);
              await browser.healthCheck();

              process.send({ type: 'ack', jobId: job.id });
            } 
            catch (err) {
              // no nack — DurableQueue's visibility timeout will automatically
              // requeue this job once its lease expires unacked
              console.error(`[browserProcess] job ${job.id} failed, letting visibility timeout requeue it:`, err.message);
            }
          }
          break;
        }

        default:
          console.log(`message type : ${msg.type}`);
      }
    });
  } catch (err) {
    console.error('[browserProcess] failed to initialize:', err);
    process.exit(1);
  }
};

browserProcess();