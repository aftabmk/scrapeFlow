const path = require('path');
const { fork } = require('child_process');

// module-level so a warm Lambda container can reuse both children
let browserChild = null;
let wsChild = null;
let wsReady = false; // tracks whether the cached wsChild's server is confirmed listening

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
  if (wsChild && !wsChild.killed && wsReady) {
    console.log('♻️  Reusing warm ws child');
    return wsChild;
  }

  const { child } = await forkAndWait(
    path.join(__dirname, 'websocket.js'),
    { cmd: 'start', port: 8080 },
    'ready'
  );

  wsChild = child;
  wsReady = true;

  wsChild.on('exit', () => {
    wsChild = null;
    wsReady = false;
  });

  return wsChild;
}

async function ensureBrowserScrape() {
  if (browserChild && !browserChild.killed) {
    return new Promise((resolve, reject) => {
      const onMessage = (msg) => {
        if (msg.type === 'done') {
          browserChild.off('message', onMessage);
          resolve(msg.data);
        } else if (msg.type === 'error') {
          browserChild.off('message', onMessage);
          reject(new Error(msg.error));
        }
      };
      browserChild.on('message', onMessage);
      browserChild.send({ cmd: 'scrape' });
    });
  }

  const { child, data } = await forkAndWait(
    path.join(__dirname, 'browser.js'),
    { cmd: 'scrape' },
    'done'
  );
  browserChild = child;
  browserChild.on('exit', () => { browserChild = null; });

  return data;
}

async function main() {
  const ws = await ensureWSChild();

  try {
    const data = await ensureBrowserScrape();

    // no longer stopping ws each run since it's cached/reused now
    // ws.send({ cmd: 'stop' });

    console.dir({ data }, { depth: 3 });
    return data;
  } catch (err) {
    console.error(err);
    throw err;
  }
}

module.exports.handler = main;
if (require.main === module) {
  require('dotenv').config({ path: path.resolve(__dirname, '../../../.env') });
  main();
}