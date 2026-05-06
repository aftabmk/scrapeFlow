'use strict';



const eventBus = require('./eventBus/eventBus');
const { Browser } = require('./browser/browser');
const { BrowserEvent } = require('./browser/browserEvent');
const { DLQEvent }     = require('./dlq/dlqEvent');
const { JobConsumer }  = require('./job/jobConsumer');
const { produceJobs }  = require('./job/jobs');


let _initPromise  = null;
let _jobConsumer  = null;


async function bootstrap(opts = {}) {
  if (_initPromise) return _initPromise;

  _initPromise = (async () => {
    // Pass eventBus as the emitter so Browser can publish browser:dlq
    Browser.getInstance({ ...opts, emitter: eventBus });
    await Browser.getInstance().init();

    // Start JobConsumer — subscribes to jobproducer:job:created
    _jobConsumer = new JobConsumer({ evaluateTimeout: opts.evaluateTimeout });
    _jobConsumer.start();

    console.info('[bootstrap] browser ready, JobConsumer started');
  })();

  return _initPromise;
}

// browser req

eventBus.subscribe('browser:request', async ({ data: event }) => {
  if (!(event instanceof BrowserEvent)) {
    eventBus.publish('consumer:error', {
      error: 'browser:request payload must be a BrowserEvent',
      received: typeof event,
    });
    return;
  }

  try {
    // Ensure browser is up even if bootstrap() hasn't been called explicitly
    await bootstrap();
    const result = await Browser.getInstance().handleEvent(event);
    eventBus.publish('browser:response', { pageId: event.pageId, result });
  } 
  catch (err) {
    // Browser already published browser:dlq for recoverable errors.
    // This covers any remaining unhandled throws.
    eventBus.publish('browser:error', { pageId: event.pageId, error: err.message });
  }
});

// ─── browser:dlq ─────────────────────────────────────────────────────────────

eventBus.subscribe('browser:dlq', async ({ data: dlq }) => {
  if (!(dlq instanceof DLQEvent)) {
    eventBus.publish('consumer:error', {
      error: 'browser:dlq payload must be a DLQEvent',
      received: typeof dlq,
    });
    return;
  }

  console.warn('[DLQ] received —', JSON.stringify(dlq.toLog()));

  const action = dlq.recoveryAction();

  // Budget exhausted — mark permanently dead
  if (action === 'discard') {
    console.error(`[DLQ] discarding "${dlq.originalEvent.pageId}" — exhausted after ${dlq.retryCount} attempts`);
    eventBus.publish('browser:dead', { pageId: dlq.originalEvent.pageId, dlq });
    return;
  }

  // Backoff then increment attempt counter
  await dlq.wait();
  dlq.recordAttempt();

  const browser = Browser.getInstance();

  try {
    if (action === 'dispose-and-replay') {
      // Tab was already disposed by Browser on TimeoutError — replay from scratch
      console.info(`[DLQ] dispose-and-replay "${dlq.originalEvent.pageId}" (attempt ${dlq.retryCount})`);
    }

    if (action === 'recreate-tab') {
      // Force evict the tab so handleEvent creates a fresh one
      console.info(`[DLQ] recreate-tab "${dlq.originalEvent.pageId}" (attempt ${dlq.retryCount})`);
      await browser._deleteTab(dlq.originalEvent.pageId);
    }

    // replay-only: reuse existing tab if alive, create new if evicted
    // All three paths re-enter via browser:request
    eventBus.publish('browser:request', dlq.originalEvent);
  } catch (err) {
    dlq.recordAttempt(err.message);
    if (dlq.canRetry()) {
      eventBus.publish('browser:dlq', dlq);
    } else {
      console.error(`[DLQ] handler error — discarding "${dlq.originalEvent.pageId}":`, err.message);
      eventBus.publish('browser:dead', { pageId: dlq.originalEvent.pageId, dlq });
    }
  }
});

// Graceful shutdown

async function shutdown() {
  if (_jobConsumer) {
    _jobConsumer.stop();
    _jobConsumer = null;
  }

  const browser = Browser.getInstance();
  if (browser) await browser.close();

  _initPromise = null;
  console.info('[shutdown] complete');
}

process.on('SIGINT',  () => shutdown().then(() => process.exit(0)));
process.on('SIGTERM', () => shutdown().then(() => process.exit(0)));


module.exports = {
  bootstrap,
  shutdown,
  produceJobs,
  eventBus,

  BrowserEvent,
  DLQEvent,
  Browser,
};