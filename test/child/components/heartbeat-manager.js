// child/components/heartbeat-manager.js
const { EventEmitter } = require('events');

class HeartbeatManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.interval = options.interval || 5000;
    this.processType = options.processType || 'generic';
    this.onHeartbeat = options.onHeartbeat || (() => ({}));
    this.timer = null;
    this.isRunning = false;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;

    this.timer = setInterval(() => {
      if (!this.isRunning) return;

      const data = this.onHeartbeat();
      this._sendHeartbeat(data);
    }, this.interval);

    console.log(`[${this.processType}] 💓 Heartbeat started (${this.interval}ms)`);
  }

  _sendHeartbeat(data) {
    if (!process.send) return;

    try {
      process.send({
        type: 'HEARTBEAT',
        ...data
      });
    } catch (error) {
      // Ignore IPC errors
    }
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    console.log(`[${this.processType}] 💓 Heartbeat stopped`);
  }
}

module.exports = HeartbeatManager;