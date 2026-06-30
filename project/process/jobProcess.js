// jobsProcess.js
const Job = require('./Job');

process.send({ type: 'ready' });

process.on('message', (msg) => {
  if (msg.type === 'start') {
    // msg.payload mimics a lambda-style event: array of { url, api }
    const jobsData = msg.payload || [];

    const jobs = jobsData.map((data, idx) =>
      new Job({ id: idx + 1, url: data.url, api: data.api })
    );

    jobs.forEach(job => process.send({ type: 'enqueue', job }));
  }

  if (msg.type === 'ack-confirm') {
    console.log(`[Jobs] job ${msg.jobId} confirmed processed`);
  }
});