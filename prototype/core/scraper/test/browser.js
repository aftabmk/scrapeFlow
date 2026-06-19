const puppeteerExtra = require('puppeteer-extra');
const StealthPlugin = require('puppeteer-extra-plugin-stealth');

puppeteerExtra.use(StealthPlugin());

let browserInstance = null;

async function launchBrowser() {
  if (browserInstance) return browserInstance;

  browserInstance = await puppeteerExtra.launch({
    headless: false,
    devtools: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-web-security',
      // '--allow-insecure-localhost',
      // '--remote-allow-origins=*',
      // '--disable-features=IsolateOrigins,site-per-process,LocalNetworkAccess,ContentSecurityPolicy',
      // '--disable-site-isolation-trials',
      // '--ignore-certificate-errors',
      // '--disable-blink-features=AutomationControlled',
      // '--disable-http2',
      // '--disable-quic',
    ],
    protocolTimeout: 60_000,
  });

  console.log('✅ Browser launched with NSE + CSP fixes');
  return browserInstance;
}

async function createPage(browser) {
  const page = await browser.newPage();
  
  await page.setBypassCSP(true);
  await page.setViewport({ width: 1366, height: 768 });
  await page.setUserAgent('Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36');

  await page.setRequestInterception(true);

  page.on('request', (req) => {
    const resourceType = req.resourceType();
    const url = req.url();

    if (resourceType === 'document' || 
        resourceType === 'fetch' || 
        resourceType === 'xhr') {
      req.continue();
      return;
    }

    if (['image', 'stylesheet', 'font', 'media', 'script', 'other'].includes(resourceType)) {
      req.abort();
      return;
    }

    req.continue(); 
  });

  console.log('✅ Page created with strict request interceptor (HTML + Fetch only)');
  return page;
}

module.exports = { launchBrowser, createPage };