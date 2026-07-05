// queueProcess.js
const DurableQueue = require('../algorithms/durableQueue/algorithm/DurableQueue');

const queue = new DurableQueue("Queue", {
  visibilityTimeout: 10_000,
  maxRetries: 3
});

const queueProcess = () => {
  process.send({ type: 'ready' });

  process.on('message', async (msg) => {
    switch (msg.type) {
      case 'enqueue':
        await queue.enqueue(msg.job);
        break;
      case 'dequeue-request':
        const jobs = [];
        const { batchSize, requestId } = msg;

        for (let i = 0; i < batchSize; i++) {
          const job = await queue.dequeue();

          if (job === null) {
            continue;
          }

          jobs.push(job);
        }

        process.send({
          type: 'dequeue-response',
          jobs,
          requestId,
        });
        break;
      case 'ack':
        queue.ack(msg.jobId);
        process.send({ type: 'ack-confirm', jobId: msg.jobId });
        break;
      default: console.log(`message type ${msg.type}`);
    }
  });
}

queueProcess();