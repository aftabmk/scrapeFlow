// test.js
require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });

const events = require('./event.json');
const { JobWorker } = require('./jobWorker');
const JobEvent      = require('../../events/jobEvent');

// ─── Job listener ────────────────────────────────────────────────────────────
JobEvent.subscribe((job) => {
  console.log('[JobEvent] received:', job);
});

// ─── Run ───────────────────────────
const worker = new JobWorker(events);
worker.run();