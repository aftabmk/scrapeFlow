const { SocketBuilder, HTMLRequest, StorageBucket } = require('./class');

async function injectClass(page) {
  const 
        HTMLRequestStr = HTMLRequest.toString(), 
        SocketBuilderStr = SocketBuilder.toString(), 
        StorageBucketStr = StorageBucket.toString();

  await page.evaluateOnNewDocument((HTMLRequestStr, SocketBuilderStr,StorageBucketStr) => {
    const 
          HTMLRequest = new Function(`return (${HTMLRequestStr})`)(), 
          SocketBuilder = new Function(`return (${SocketBuilderStr})`)(),
          StorageBucket = new Function(`return (${StorageBucketStr})`)();
    // Inject the original functions into page context
    const initialiseOnLoad = () => {
      window.SocketBuilder = new SocketBuilder, 
      window.HTMLRequest = new HTMLRequest, 
      window.StorageBucket = new StorageBucket;
    }

    window.addEventListener('DOMContentLoaded', initialiseOnLoad);
  }, HTMLRequestStr, SocketBuilderStr, StorageBucketStr);

  console.log("class injected");
}

async function injectWebSocket(page, wsPort, pageId) {
  await page.evaluateOnNewDocument((port, id) => {
    window.pageId = id;

    const initialiseOnLoad = () => {
      window.SocketBuilder.create(port, id);
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

  return await page.evaluate((endpoint) => {
    return window.HTMLRequest.fetch(endpoint);
  }, endpoint);
}

module.exports = { inject, triggerFetch };