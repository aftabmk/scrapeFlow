// test.js
// require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });
require('dotenv').config();

const TradeBuilder = require('./models/tradeBuilder');
const events = require('./event.json');
const { JobWorker } = require('./jobWorker');
const JobEvent      = require('../../events/jobEvent');

// ─── Job listener ────────────────────────────────────────────────────────────
JobEvent.subscribe((job) => {
  console.log('[JobEvent] received:', job);
  const data = TradeBuilder.decode(job.no)
  console.log({decode : data})
});

// ─── Run ───────────────────────────
const worker = new JobWorker(events);
worker.run();