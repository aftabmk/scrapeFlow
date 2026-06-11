// test.js
require('dotenv').config({ path: require('path').resolve(__dirname, '../../.env') });

const Browser  = require('./core/browser/browser');

const { JobWorker } = require('../job/jobWorker')

const JobEvent = require('../../events/jobEvent');
const ScraperEvent = require('../../events/scraperEvent');


async function main() {
  // --- boot ---
  const browser = Browser.getInstance();
  await browser.init();

  // --- health before jobs ---
  console.log('\n[test] initial health check');
  await browser.healthCheck();

  // subscrbe to browser scraping events
  ScraperEvent.subscribe((job) => {
    console.log('[ScraperEvent] received:', job);
  });

  // ─── Job listener ────────────────────────────────────────────────────────────
  JobEvent.subscribe((job) => {
    console.log('[JobEvent] received:', job);
  });

  // ─── Run ─────────────────────────────────────────────────────────────────────
  const worker = new JobWorker();
  worker.run();

  // --- health after jobs ---
  console.log('\n[test] health check after jobs');
  await browser.healthCheck();

  // --- teardown ---
  // console.log('\n[test] closing browser');
  // await browser.close();
}

main().catch(console.error);