// parent/components/health-monitor.js
const { EventEmitter } = require('events');

class HealthMonitor extends EventEmitter {
  constructor(processManager, options = {}) {
    super();
    this.processManager = processManager;
    this.heartbeatTimeout = options.heartbeatTimeout || 15000;
    this.checkInterval = options.checkInterval || 2000;
    this.isRunning = true;
    this.timer = null;
    
    this._startMonitoring();
  }

  _startMonitoring() {
    this.timer = setInterval(() => {
      if (!this.isRunning) return;
      
      const now = Date.now();
      const processes = this.processManager.getAllProcesses();
      
      for (const info of processes) {
        if (info.status === 'running') {
          const elapsed = now - info.lastHeartbeat;
          
          if (elapsed > this.heartbeatTimeout) {
            console.warn(`[HealthMonitor] ⏰ Process ${info.pid} heartbeat timeout (${elapsed}ms)`);
            this.emit('heartbeatTimeout', { pid: info.pid, info });
          }
        }
      }
    }, this.checkInterval);
  }

  updateHeartbeat(pid) {
    const process = this.processManager.getAllProcesses().find(p => p.pid === pid);
    if (process) {
      process.lastHeartbeat = Date.now();
    }
  }

  shutdown() {
    this.isRunning = false;
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }
}

module.exports = HealthMonitor;