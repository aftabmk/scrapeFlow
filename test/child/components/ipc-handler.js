// child/components/ipc-handler.js
const { EventEmitter } = require('events');

class IPCHandler extends EventEmitter {
  constructor(options = {}) {
    super();
    this.processType = options.processType || 'generic';
    this.onMessage = options.onMessage || null;
    this.isRunning = false;
    this.messageQueue = [];
  }

  start() {
    this.isRunning = true;
    this._setupListener();
    console.log(`[${this.processType}] 📨 IPC Handler started`);
  }

  _setupListener() {
    process.on('message', async (message) => {
      if (!this.isRunning) return;

      if (this.onMessage) {
        await this.onMessage(message);
      }

      this.emit('message', message);
    });
  }

  send(message) {
    if (!process.send) {
      console.log(`[${this.processType}] ⚠️ process.send not available`);
      return;
    }

    try {
      process.send(message);
    } catch (error) {
      console.error(`[${this.processType}] ❌ IPC send error:`, error.message);
    }
  }

  sendQueued() {
    while (this.messageQueue.length > 0) {
      const message = this.messageQueue.shift();
      this.send(message);
    }
  }

  stop() {
    this.isRunning = false;
    this.messageQueue = [];
    console.log(`[${this.processType}] 📨 IPC Handler stopped`);
  }
}

module.exports = IPCHandler;