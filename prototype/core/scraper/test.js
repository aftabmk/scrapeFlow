// test.js
require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });

const events = require('../../event.json');
const Browser  = require('./core/browser/browser');
const { JobWorker } = require('../job/jobWorker')

const JobEvent = require('../../events/jobEvent');
const ScraperEvent = require('../../events/scraperEvent');

async function main() {
  try {
    // --- boot ---
    const browser = Browser.getInstance();
    await browser.init();

    // --- health before jobs ---
    console.log('\n[test] initial health check');
    await browser.healthCheck();

    // Subscribe to events
    ScraperEvent.subscribe((job) => {
      console.log('[ScraperEvent] received:', job.id);
    });

    JobEvent.subscribe((job) => {
      console.log('[JobEvent] received:', job);
    });

    const worker = new JobWorker(events);

    // ─── Run worker every 30 seconds, 5 times ─────────────────────────────────
    console.log('\n🚀 Starting JobWorker - will run 5 times every 30s');

    let runCount = 0;
    const MAX_RUNS = 2, INTERVAL_MS = 30 * 1000; // 30 seconds

    const intervalId = setInterval(async () => {
      runCount++;
      console.log(`\n[Run ${runCount}/${MAX_RUNS}] Starting worker.run()`);

      try {
        await worker.run();           // Make sure run() is async if needed
        console.log(`[Run ${runCount}/${MAX_RUNS}] worker.run() completed`);
      } 
      catch (err) {
        console.error(`[Run ${runCount}/${MAX_RUNS}] Error:`, err.message);
      }

      // Stop after 5 runs
      if (runCount >= MAX_RUNS) {
        clearInterval(intervalId);
        console.log('\n✅ Completed 5 runs of worker.run()');
        
        // Final health check
        console.log('\n[test] final health check');
        await browser.healthCheck();
      }
    }, INTERVAL_MS);

    // Also run immediately (first run)
    console.log('\n[Run 1/5] Initial worker.run()');
    await worker.run();

  } 
  catch(e) {
    console.error({ error: e.message });
  }
}

main();

