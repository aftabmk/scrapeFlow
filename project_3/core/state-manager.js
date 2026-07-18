// core/state-manager.js
const { EventEmitter } = require('events');

class StateManager extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = { checkpointInterval: 5000, maxHistory: 100, ...options };
    
    this.state = {
      version: '3.0.0',
      system: { status: 'starting', startTime: Date.now(), uptime: 0 },
      workers: {},
      queues: {},
      jobs: {},
      pipeline: { status: 'idle', currentStage: null, progress: 0 },
      health: { status: 'unknown', components: {} },
    };
    
    this.history = [];
    this.checkpoints = [];
    this.persistence = null;
    this.persistenceReady = false;
    this.stats = { updates: 0, checkpoints: 0, rollbacks: 0 };
    
    this.startCheckpointing();
    console.log('[StateManager] Initialized');
  }

  getState() { return this.state; }

  update(path, value) {
    const oldValue = this.getPath(path);
    this.setPath(path, value);
    this.stats.updates++;
    this.history.push({ path, oldValue, newValue: value, timestamp: Date.now() });
    if (this.history.length > this.options.maxHistory) this.history.shift();
    this.emit('state.updated', { path, value });
    return this;
  }

  getPath(path) {
    const parts = path.split('.');
    let current = this.state;
    for (const part of parts) {
      if (current === undefined || current === null) return undefined;
      current = current[part];
    }
    return current;
  }

  setPath(path, value) {
    const parts = path.split('.');
    const last = parts.pop();
    let current = this.state;
    for (const part of parts) {
      if (!current[part]) current[part] = {};
      current = current[part];
    }
    current[last] = value;
  }

  updateWorker(workerId, data) {
    if (!this.state.workers[workerId]) this.state.workers[workerId] = {};
    Object.assign(this.state.workers[workerId], data);
    this.state.workers[workerId].updatedAt = Date.now();
    this.emit('worker.updated', { workerId, data });
    return this;
  }

  updateJob(jobId, data) {
    if (!this.state.jobs[jobId]) this.state.jobs[jobId] = {};
    Object.assign(this.state.jobs[jobId], data);
    this.state.jobs[jobId].updatedAt = Date.now();
    this.emit('job.updated', { jobId, data });
    return this;
  }

  checkpoint() {
    const checkpoint = { id: `ckpt_${Date.now()}`, state: JSON.parse(JSON.stringify(this.state)), timestamp: Date.now(), stats: { ...this.stats } };
    this.checkpoints.push(checkpoint);
    if (this.checkpoints.length > 10) this.checkpoints.shift();
    this.stats.checkpoints++;
    if (this.persistenceReady && this.persistence) this.persistence.saveCheckpoint(checkpoint);
    this.emit('checkpoint.created', { id: checkpoint.id });
    return checkpoint;
  }

  getSystemStatus() { return { status: this.state.system.status, uptime: Date.now() - this.state.system.startTime, version: this.state.version }; }
  getHealth() {
    const components = this.state.health.components;
    const statuses = Object.values(components);
    if (statuses.length === 0) return { status: 'unknown' };
    const healthy = statuses.filter(s => s === 'healthy').length;
    const unhealthy = statuses.filter(s => s === 'unhealthy').length;
    const degraded = statuses.filter(s => s === 'degraded').length;
    let status = 'healthy';
    if (unhealthy > 0) status = 'unhealthy';
    else if (degraded > 0) status = 'degraded';
    return { status, healthy, unhealthy, degraded, total: statuses.length };
  }

  getPipelineStatus() { return { status: this.state.pipeline.status, currentStage: this.state.pipeline.currentStage, progress: this.state.pipeline.progress }; }
  startCheckpointing() { this.checkpointTimer = setInterval(() => this.checkpoint(), this.options.checkpointInterval); }

  setPersistence(provider) { this.persistence = provider; this.persistenceReady = true; console.log('[StateManager] Persistence enabled'); }

  getStats() {
    return { updates: this.stats.updates, checkpoints: this.stats.checkpoints, rollbacks: this.stats.rollbacks, historySize: this.history.length, checkpointsSize: this.checkpoints.length, workers: Object.keys(this.state.workers).length, jobs: Object.keys(this.state.jobs).length, queues: Object.keys(this.state.queues).length };
  }

  shutdown() {
    console.log('[StateManager] Shutting down...');
    clearInterval(this.checkpointTimer);
    this.checkpoint();
    this.removeAllListeners();
    console.log('[StateManager] Shutdown complete');
  }
}

module.exports = StateManager;