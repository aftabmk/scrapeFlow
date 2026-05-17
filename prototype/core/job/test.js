require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });

const TracerEvent   = require('../../events/tracerEvent');
const JobEvent      = require('../../events/jobEvent');
const { JobWorker } = require('./jobWorker');
 
TracerEvent.subscribe(
  ({ key, chain })          => console.log(`[Tracer] ${key} → ${chain.join(' > ')}`),
  ({ key, chain, message }) => console.warn(`[Tracer:warn] ${key} → ${chain.join(' > ')} | ${message}`)
);
 
JobEvent.subscribe((job) => {
  console.log('[JobEvent] received:', job);
});
 
const worker = new JobWorker();
worker.run();