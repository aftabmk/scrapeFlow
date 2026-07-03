// browserProcess.js
const { Job } = require('../core/job/models/job');
const Browser = require('../core/scraper/core/browser/browser');

const browserProcess = async () => {
  try {
    const browser = Browser.getInstance();
    // await browser.init();

    process.send({ type: 'ready' });

    process.on('message', async (msg) => {
      switch (msg.type) {
        case 'trigger-poll':
          process.send({ type: 'dequeue-request', batchSize: 4, requestId: Date.now() });
          break;

        case 'dequeue-response': {
          const jobs = msg.jobs.map(raw =>
            Object.assign(Object.create(Job.prototype), raw)
          );

          for (const job of jobs) {
            try {
              const decoded = job.decode();

              // await browser.runJob(decoded); // builds tab, loads job.url, fetches job.api
              // await browser.healthCheck();

              process.send({ type: 'ack', jobId: job.id });
            } catch (err) {
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