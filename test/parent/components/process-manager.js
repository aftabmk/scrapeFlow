// parent/components/process-manager.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class ProcessManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.processes = new Map();
    this.restartDelay = options.restartDelay || 2000;
    this.isRunning = true;
  }

  createProcess(options = {}) {
    const {
      type = 'browser',
      processingWorkers = 2,
      queueName = `${type}_queue`,
      args = []
    } = options;

    const scriptMap = {
      browser: './child/browser.js',
      analyzer: './child/analyzer.js',
      exporter: './child/exporter.js',
      'job-submitter': './child/job-submitter.js'
    };
    
    const scriptPath = scriptMap[type] || scriptMap.browser;
    const fullPath = path.join(__dirname, '../..', scriptPath);

    const child = fork(fullPath, [
      `--processing-workers=${processingWorkers}`,
      `--queue-name=${queueName}`,
      `--process-type=${type}`,
      ...args
    ], {
      stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      execArgv: ['--experimental-sqlite']
    });

    const processInfo = {
      pid: child.pid,
      child,
      type,
      processingWorkers,
      queueName,
      lastHeartbeat: Date.now(),
      status: 'starting',
      createdAt: Date.now(),
      restartCount: 0,
      isReady: false
    };

    child.on('message', (msg) => {
      this.emit('message', processInfo, msg);
    });

    child.on('exit', (code, signal) => {
      this.emit('exit', processInfo, code, signal);
    });

    child.on('error', (err) => {
      console.error(`[ProcessManager] Child ${child.pid} error:`, err);
      this.emit('exit', processInfo, -1, 'error');
    });

    this.processes.set(child.pid, processInfo);

    return new Promise((resolve) => {
      const readyHandler = (msg) => {
        if (msg.type === 'ready' && msg.processType === type) {
          processInfo.status = 'running';
          processInfo.isReady = true;
          
          console.log(`[ProcessManager] ✅ Process ${child.pid} (${type}) ready`);
          this.emit('processReady', { pid: child.pid, type, ...msg });
          
          resolve(processInfo);
          child.off('message', readyHandler);
        }
      };
      
      child.on('message', readyHandler);
      
      setTimeout(() => {
        child.off('message', readyHandler);
        if (processInfo.status === 'starting') {
          processInfo.status = 'timeout';
          console.warn(`[ProcessManager] Process ${child.pid} (${type}) startup timeout`);
          this.emit('processTimeout', { pid: child.pid, type });
          resolve(processInfo);
        }
      }, 15000);
    });
  }

  async restartProcess(processInfo) {
    const { type, processingWorkers, queueName } = processInfo;
    
    console.log(`[ProcessManager] 🔄 Restarting process ${processInfo.pid} (${type})...`);
    
    this.processes.delete(processInfo.pid);
    
    await this._sleep(this.restartDelay);
    
    const newProcess = await this.createProcess({
      type,
      processingWorkers,
      queueName
    });
    
    console.log(`[ProcessManager] ✅ Process ${processInfo.pid} restarted as ${newProcess.pid}`);
    return newProcess;
  }

  getProcess(type) {
    for (const [pid, info] of this.processes) {
      if (info.type === type && info.status === 'running' && info.isReady) {
        return info;
      }
    }
    return null;
  }

  getAllProcesses() {
    return Array.from(this.processes.values());
  }

  getProcessCount() {
    return this.processes.size;
  }

  getReadyCount() {
    return Array.from(this.processes.values())
      .filter(p => p.isReady).length;
  }

  isAllReady() {
    return this.getReadyCount() === this.processes.size && this.processes.size > 0;
  }

  async waitForProcess(type, timeout = 30000) {
    const startTime = Date.now();
    
    while (Date.now() - startTime < timeout) {
      const process = this.getProcess(type);
      if (process) return process;
      await this._sleep(500);
    }
    return null;
  }

  async shutdown() {
    this.isRunning = false;
    
    const promises = [];
    for (const [pid, info] of this.processes) {
      promises.push(new Promise((resolve) => {
        try {
          info.child.send({ type: 'SHUTDOWN' });
        } catch (err) {
			console.log({message : err.message});
		}
        info.child.on('exit', resolve);
        setTimeout(resolve, 3000);
      }));
    }
    
    await Promise.all(promises);
    this.processes.clear();
  }

  _sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}

module.exports = ProcessManager;