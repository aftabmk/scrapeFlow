// queueProcess.js
const DurableQueue = require('../algorithms/durableQueue/algorithm/DurableQueue');

const queue = new DurableQueue("Queue", {
      visibilityTimeout: 10000,
      maxRetries: 3
  });

process.send({ type: 'ready' });

process.on('message', async (msg) => {
  switch(msg.type) {
    case 'enqueue' :
      await queue.enqueue(msg.job);
      break;
    case 'dequeue-request' :
      let size = msg.batchSize, jobs = [];
      while(size --) {
        const job = await queue.dequeue(msg.batchSize);
        jobs.push(job);
      }
      process.send({ type: 'dequeue-response', jobs, requestId: msg.requestId });
      break;
    case 'ack' :
      queue.ack(msg.jobId);
      process.send({ type: 'ack-confirm', jobId: msg.jobId });
      break;
    default : console.log(`message type ${msg.type}`);
  }
});