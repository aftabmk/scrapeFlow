const { TracerStore }  = require('./tracerStore');
const TracerEvent = require('../../events/tracerEvent');

// ─── Subscribe ───────────────────────────────────────────────────────────────
TracerEvent.subscribe((data) => {
  console.log('[TracerEvent]', data);
});

// ─── Trace ───────────────────────────────────────────────────────────────────
TracerEvent.trace({ jobId: 1, class: 'Job', function: '_validate', status: 'success' });
TracerEvent.trace({ jobId: 1, class: 'JobWorker', function: 'run', status: 'success' });
TracerEvent.trace({ jobId: 2, class: 'Job', function: '_validate', status: 'failure', message: 'missing page_url' });
TracerEvent.trace({ jobId: 2, class: 'JobWorker', function: 'run', status: 'failure', message: 'no valid jobs' });

// ─── traceWithId ─────────────────────────────────────────────────────────────
console.log('\n--- traceWithId(1) ---');
console.log(TracerStore.traceWithId(1));

console.log('\n--- traceWithId(2) ---');
console.log(TracerStore.traceWithId(2));

console.log('\n--- traceAll ---------');
console.log(TracerStore.traceAll());
