const path = require('path');
const { fork } = require('child_process');

const cache = new Map();

function forkAndWait(workerPath, sendMsg, readyType) {
  return new Promise((resolve, reject) => {
    const child = fork(workerPath);

    const onMessage = (msg) => {
      if (msg.type === readyType) {
        child.off('message', onMessage);
        resolve({ child, data: msg.data });
      } else if (msg.type === 'error') {
        child.off('message', onMessage);
        reject(new Error(msg.error));
      }
    };

    child.on('message', onMessage);
    child.on('error', reject);
    child.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
    });

    child.send(sendMsg);
  });
}

async function ensureWSChild() {
  const cachedChild = cache.get('wsChild');
  const cachedReady = cache.get('wsReady');

  if (cachedChild && !cachedChild.killed && cachedReady) {
    console.log('♻️  Reusing warm ws child');
    return cachedChild;
  }

  const { child } = await forkAndWait(
    path.join(__dirname, 'workers', 'websocketWorker.js'),
    { cmd: 'start', port: 8080 },
    'ready'
  );

  cache.set('wsChild', child);
  cache.set('wsReady', true);

  child.on('exit', () => {
    cache.set('wsChild', null);
    cache.set('wsReady', false);
  });

  return child;
}

async function ensureBrowserScrape() {
  const cachedChild = cache.get('browserChild');

  if (cachedChild && !cachedChild.killed) {
    return new Promise((resolve, reject) => {
      const onMessage = (msg) => {
        if (msg.type === 'done') {
          cachedChild.off('message', onMessage);
          resolve(msg.data);
        } else if (msg.type === 'error') {
          cachedChild.off('message', onMessage);
          reject(new Error(msg.error));
        }
      };
      cachedChild.on('message', onMessage);
      cachedChild.send({ cmd: 'scrape' });
    });
  }

  const { child, data } = await forkAndWait(
    path.join(__dirname, 'workers', 'browserWorker.js'),
    { cmd: 'scrape' },
    'done'
  );

  cache.set('browserChild', child);

  child.on('exit', () => {
    cache.set('browserChild', null);
  });

  return data;
}

module.exports = { ensureWSChild, ensureBrowserScrape };