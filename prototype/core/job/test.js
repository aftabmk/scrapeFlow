// test.js
require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });

const JobEvent      = require('../../events/jobEvent');
const { JobWorker } = require('./jobWorker');

// ─── Job listener ────────────────────────────────────────────────────────────
JobEvent.subscribe((job) => {
  console.log('[JobEvent] received:', job);
});

// ─── Run ─────────────────────────────────────────────────────────────────────
const worker = new JobWorker();
worker.run();