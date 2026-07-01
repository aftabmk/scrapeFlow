// browserProcess.js
const Browser = require('../core/scraper/core/browser/browser');

const main = async () => {
  try {
    const browser = Browser.getInstance();
    // await browser.init();

    process.send({ type: 'ready' });

    process.on('message', async (msg) => {
      if (msg.type === 'trigger-poll') {
        process.send({ type: 'dequeue-request', batchSize: 4, requestId: Date.now() });
      }

      // if (msg.type === 'dequeue-response') {
      //   for (const job of msg.jobs) {
      //     try {
      //       // await browser.runJob(job); // builds tab, loads job.url, fetches job.api
      //       process.send({ type: 'ack', jobId: job.id });
      //     } catch (err) {
      //       process.send({ type: 'nack', jobId: job.id });
      //     }
      //   }
      // }
    });
  } catch (err) {
    console.error('[browserProcess] failed to initialize:', err);
    process.exit(1);
  }
};

main();