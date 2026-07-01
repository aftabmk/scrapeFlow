// queueProcess.js
const DurableQueue = require('../algorithms/durableQueue/algorithm/DurableQueue');

const queue = new DurableQueue("Queue", {
      visibilityTimeout: 10000,
      maxRetries: 3
  });

process.send({ type: 'ready' });

process.on('message', async (msg) => {
  if (msg.type === 'enqueue') {
    await queue.enqueue(msg.job);
  }

  if (msg.type === 'dequeue-request') {
    const jobs = await queue.dequeue(msg.batchSize);
    process.send({ type: 'dequeue-response', jobs, requestId: msg.requestId });
  }

  if (msg.type === 'ack') {
    queue.ack(msg.jobId);
    process.send({ type: 'ack-confirm', jobId: msg.jobId });
  }

  if (msg.type === 'nack') {
    queue.nack(msg.jobId);
  }
});