'use strict';

const { parentPort, workerData } = require('worker_threads');


const HandlerClass = require(workerData.handlerPath);
const handler = new HandlerClass(workerData.handlerOptions || {});

parentPort.on('message', async (msg) => {
  const { job } = msg;
  const startedAt = Date.now();

  try {
    const result = await handler.run(job);
    const finishedAt = Date.now();

    parentPort.postMessage({
      ok: true,
      job,
      result,
      trace: {
        jobId: job.id,
        event: 'worker.complete',
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
      },
    });
  } 
  catch (err) {
    const finishedAt = Date.now();

    parentPort.postMessage({
      ok: false,
      job,
      error: { message: err.message, stack: err.stack },
      trace: {
        jobId: job.id,
        event: 'worker.error',
        startedAt,
        finishedAt,
        durationMs: finishedAt - startedAt,
      },
    });
  }
});