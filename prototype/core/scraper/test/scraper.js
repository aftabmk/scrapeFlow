const { SocketBuilder, HTMLRequest } = require('./class');
// Convert functions to injectable strings

async function injectClass(page) {
  const HTMLRequestStr = HTMLRequest.toString(), SocketBuilderStr = SocketBuilder.toString();

  await page.evaluateOnNewDocument((HTMLRequestStr, SocketBuilderStr) => {
    const HTMLRequest = new Function(`return (${HTMLRequestStr})`)(), 
          SocketBuilder = new Function(`return (${SocketBuilderStr})`)();
    // Inject the original functions into page context
    const initialiseOnLoad = () => {
      window.SocketBuilder = new SocketBuilder, window.HTMLRequest = new HTMLRequest;
    }

    window.addEventListener('DOMContentLoaded', initialiseOnLoad);
  }, HTMLRequestStr, SocketBuilderStr);

  console.log("class injected");
}

async function injectWebSocket(page, wsPort, pageId) {
  await page.evaluateOnNewDocument((port, id) => {
    window.pageId = id;

    const initialiseOnLoad = () => {
      const socket = window.SocketBuilder.create(port, id);

      socket.onopen = () => console.log(`✅ WS Connected: ${id}`);
      socket.onerror = (e) => console.error(`WS Error ${id}:`, e);
      socket.onclose = () => console.log(`WS Closed: ${id}`);
    };

    window.addEventListener('DOMContentLoaded', initialiseOnLoad);
  }, wsPort, pageId);

  console.log("socket created and injected");
}

async function inject(page, wsPort = 8080, pageId = 'default') {
  await injectClass(page);
  await injectWebSocket(page, wsPort, pageId);

  console.log(`✅ Injected scraper for ${pageId}`);
}

const checkInjection = async (page, timeout) => {
  console.log('checking injection');

  await page.evaluate(timeout => new Promise((resolve, reject) => {
        if (!window.HTMLRequest ||typeof window.HTMLRequest.fetch !== 'function')
          return reject('HTMLRequest unavailable');

        if (!window.socket)
          return reject('SocketBuilder unavailable');

        if (window.socket.readyState === WebSocket.OPEN)
          return resolve();

        const timer = setTimeout(() => reject('Timeout'), timeout);

        window.socket.addEventListener('open',() => {
            clearTimeout(timer);
            resolve();
          },{ once: true }
        );

        window.socket.addEventListener('error',() => {
            clearTimeout(timer);
            reject('SocketBuilder error');
          },{ once: true }
        );
      }),
    timeout
  );
  console.log('injection passed');
}

async function triggerFetch(page, endpoint, timeout = 30_000) {
  console.log(`🚀 Triggering fetch: ${endpoint}`);
  await checkInjection(page, timeout);

  return await page.evaluate((ep) => {
    return window.HTMLRequest.fetch(ep);
  }, endpoint);
}

module.exports = { inject, triggerFetch };