// test.js
require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });

const TracerEvent   = require('../../events/tracerEvent');
const JobEvent      = require('../../events/jobEvent');
const { JobWorker } = require('./jobWorker');

// ─── Tracer listener ─────────────────────────────────────────────────────────
TracerEvent.subscribe(({ jobId, class: cls, function: fn, status, message }) => {
  const msg = `[Tracer] jobId=${jobId} | ${cls} > ${fn} > ${status}${message ? ` | ${message}` : ''}`;
  status === 'failure' ? console.warn(msg) : console.log(msg);
});

// ─── Job listener ────────────────────────────────────────────────────────────
JobEvent.subscribe((job) => {
  console.log('[JobEvent] received:', job);

  // Print all traces for this job
  console.log(`[Tracer] all traces for jobId=${job.id}:`, TracerEvent.traceWithId(job.id));
});

// ─── Run ─────────────────────────────────────────────────────────────────────
const worker = new JobWorker();
worker.run();