const { fork } = require('child_process');

class BaseWorkerManager {
  static cache = new Map();
  static key = null;
  static readyType = null;
  static reuseWarm = false;

  static forkAndWait(workerPath, sendMsg, readyType) {
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
      child.on('exit', (code) => {
        if (code !== 0) reject(new Error(`Worker exited with code ${code}`));
      });

      child.send(sendMsg);
    });
  }

  static sendAndWait(child, sendMsg, successType) {
    return new Promise((resolve, reject) => {
      const onMessage = (msg) => {
        if (msg.type === successType) {
          child.off('message', onMessage);
          resolve(msg.data);
        } else if (msg.type === 'error') {
          child.off('message', onMessage);
          reject(new Error(msg.error));
        }
      };
      child.on('message', onMessage);
      child.send(sendMsg);
    });
  }

  static async ensure(workerPath, sendMsg) {
    const cached = this.cache.get(this.key);

    if (cached && !cached.killed) {
      if (this.reuseWarm) {
        console.log(`♻️  Reusing warm '${this.key}' worker`);
        return { child: cached, data: null };
      }
      const data = await this.sendAndWait(cached, sendMsg, this.readyType);
      return { child: cached, data };
    }

    const { child, data } = await this.forkAndWait(workerPath, sendMsg, this.readyType);

    this.cache.set(this.key, child);

    return { child, data };
  }
}

module.exports = { BaseWorkerManager };