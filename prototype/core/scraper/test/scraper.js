async function injectWebSocketScraper(page, wsPort = 8080, pageId = 'default') {
  await page.evaluateOnNewDocument((port, id) => {
    const initScraper = () => {
      if (window.scraper) return;

      const socket = new WebSocket(`ws://localhost:${port}/${id}`);

      window.scraper = {
        socket,

        async fetchJson(endpoint, extraHeaders = {}) {
          try {
            console.log(`🌐 Fetching: ${endpoint}`);

            const headers = {
              'Content-Type': 'application/json',
              'Accept': 'application/json, text/plain, */*',
              'User-Agent': navigator.userAgent,
              'Referer': location.href,
              'Origin': location.origin,
              ...extraHeaders
            };

            const response = await fetch(endpoint, {
              method: 'GET',
              headers,
              credentials: 'include'
            });

            if (!response.ok) throw new Error(`HTTP ${response.status}`);

            const data = await response.json();

            socket.send(JSON.stringify({
              type: 'success',
              endpoint,
              data,
              timestamp: new Date().toISOString(),
              pageUrl: location.href,
              pageId: id
            }));

            return data;
          } catch (error) {
            console.error('❌ Fetch error:', error.message);
            socket.send(JSON.stringify({
              type: 'error',
              endpoint,
              message: error.message,
              pageUrl: location.href,
              pageId: id
            }));
            throw error;
          }
        }
      };

      socket.onopen = () => console.log(`✅ WS Connected: ${id}`);
      socket.onerror = (e) => console.error(`WS Error ${id}:`, e);
      socket.onclose = () => console.log(`WS Closed: ${id}`);
    };

    initScraper();
    setTimeout(initScraper, 500);
    setTimeout(initScraper, 1500);
  }, wsPort, pageId);

  console.log(`✅ Injected scraper for ${pageId}`);

  await page.evaluate(() => {
    if (!window.scraper) {
      console.warn('Scraper not found after navigation, forcing injection...');
    }
  });
}

async function triggerFetch(page, endpoint, timeout = 30_000) {
  console.log(`🚀 Triggering fetch: ${endpoint}`);

  await page.waitForFunction(() => window.scraper && typeof window.scraper.fetchJson === 'function', {
    timeout: timeout
  });

  await page.evaluate((ep) => {
    return window.scraper.fetchJson(ep);
  }, endpoint);
}

module.exports = { injectWebSocketScraper, triggerFetch };