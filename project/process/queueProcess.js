// queueProcess.js
const DurableQueue = require('./DurableQueue');

const queue = new DurableQueue();

process.send({ type: 'ready' });

process.on('message', (msg) => {
  if (msg.type === 'enqueue') {
    queue.enqueue(msg.job);
  }

  if (msg.type === 'dequeue-request') {
    const jobs = queue.dequeue(msg.batchSize);
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