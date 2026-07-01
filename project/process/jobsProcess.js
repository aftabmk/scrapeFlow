// jobsProcess.js
const { JobBuilder } = require('../core/job/models/jobBuilder');

process.send({ type: 'ready' });

process.on('message', (msg) => {
  if (msg.type === 'start') {
    // msg.payload mimics a lambda-style event: array of { url, api }
    const data = msg.payload || [];

    const jobBuilder = new JobBuilder(data);
    const jobs = jobBuilder.buildAll();
    jobs.forEach(job => process.send({ type: 'enqueue', job }));
  }

  if (msg.type === 'ack-confirm') {
    console.log(`[Jobs] job ${msg.jobId} confirmed processed`);
  }
});