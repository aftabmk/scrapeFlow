const BrowserManager = require('./browserManager.js');
const { inject, triggerFetch } = require('./injection');
const { PAGE_URL_1, API_URL_1, PAGE_URL_3, API_URL_3 } = process.env;

// url -> page (kept alive across warm invocations)
const pageCache = new Map();
let launched = false;

async function getPage(url) {
  if (pageCache.has(url)) {
    console.log(`♻️  Reusing cached page for: ${url}`);
    return pageCache.get(url);
  }

  const page = await BrowserManager.createPage();
  pageCache.set(url, page);
  return page;
}

async function scrape() {
  if (!launched) {
    await BrowserManager.launch();
    launched = true;
  }

  const isPage1New = !pageCache.has(PAGE_URL_1);
  const isPage3New = !pageCache.has(PAGE_URL_3);

  const page1 = await getPage(PAGE_URL_1);
  const page2 = await getPage(PAGE_URL_3);

  if (isPage1New) await inject(page1, 8080, 'page1');
  if (isPage3New) await inject(page2, 8080, 'page2');

  await Promise.all([
    isPage1New
      ? page1.goto(PAGE_URL_1, { waitUntil: 'domcontentloaded' })
      : Promise.resolve(),
    isPage3New
      ? page2.goto(PAGE_URL_3, { waitUntil: 'domcontentloaded' })
      : Promise.resolve(),
  ]);

  console.log('\n🎯 WebSocket Scraper Ready!');

  // no page.close() — pages stay alive for reuse on next warm invocation
  return Promise.all([
    triggerFetch(page1, API_URL_1),
    triggerFetch(page2, API_URL_3),
  ]);
}

module.exports = { scrape };