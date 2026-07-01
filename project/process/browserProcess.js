// browserProcess.js
const { Job } = require('../core/job/models/job');
const Browser = require('../core/scraper/core/browser/browser');

const main = async () => {
  try {
    const browser = Browser.getInstance();
    // await browser.init();

    process.send({ type: 'ready' });

    process.on('message', async (msg) => {
      switch(msg.type) {
        case 'trigger-poll' : 
          process.send({ type: 'dequeue-request', batchSize: 4, requestId: Date.now() });
          break;

        case 'dequeue-response' : 
          const job = Object.assign(Object.create(Job.prototype), msg.jobs);
          const id = job.decode();
    
          console.log('end');
          // for (const job of msg.jobs) {
          //   try {
          //     // await browser.runJob(job); // builds tab, loads job.url, fetches job.api
          //     process.send({ type: 'ack', jobId: job.id });
          //   } catch (err) {
          //     process.send({ type: 'nack', jobId: job.id });
          //   }
          // }
          break;

        default : console.log(`message type : ${msg.type}`)
      }

    });
  } 
  catch (err) {
    console.error('[browserProcess] failed to initialize:', err);
    process.exit(1);
  }
};

main();