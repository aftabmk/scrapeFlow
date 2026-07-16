// core/load-balancer.js
const { EventEmitter } = require('events');

class LoadBalancer extends EventEmitter {
  constructor(options = {}) {
    super();
    
    this.options = {
      maxQueueSize: 10000,
      batchSize: 50,
      workerTimeout: 30000,
      ...options,
    };
    
    this.queues = { high: [], normal: [], low: [] };
    this.workers = {
      submitter: [],
      analyzer: [],
      browser: [],
      exporter: [],
    };
    
    this.workerRegistry = new Map();
    this.roundRobinIndex = 0;
    this.processing = false;
    this.isRunning = true;
    
    this.stats = {
      tasksQueued: 0,
      tasksProcessed: 0,
      tasksFailed: 0,
      tasksRejected: 0,
      tasksRouted: 0,
    };
    
    this.startProcessing();
    
    console.log('[LoadBalancer] Initialized (Health checks disabled)');
  }

  enqueue(task, priority = 'normal') {
    if (!this.isRunning) return null;
    if (!this.queues[priority]) priority = 'normal';
    
    const wrappedTask = {
      ...task,
      id: task.id || this.generateId(),
      priority,
      enqueuedAt: Date.now(),
      attempts: 0,
      maxAttempts: task.maxAttempts || 3,
      status: 'pending',
    };
    
    // Ensure workerType is set
    if (wrappedTask.type === 'execute' && !wrappedTask.workerType) {
      if (wrappedTask.payload && wrappedTask.payload.workerType) {
        wrappedTask.workerType = wrappedTask.payload.workerType;
      } else {
        wrappedTask.workerType = 'submitter';
      }
    }
    
    const totalSize = this.getQueueSize();
    if (totalSize >= this.options.maxQueueSize) {
      this.stats.tasksRejected++;
      this.emit('queue.full', { task: wrappedTask });
      return null;
    }
    
    this.queues[priority].push(wrappedTask);
    this.stats.tasksQueued++;
    this.emit('task.enqueued', wrappedTask);
    
    return wrappedTask.id;
  }

  dequeue() {
    if (this.queues.high.length > 0) return this.queues.high.shift();
    if (this.queues.normal.length > 0) return this.queues.normal.shift();
    if (this.queues.low.length > 0) return this.queues.low.shift();
    return null;
  }

  registerWorker(worker, type) {
    const workerId = `worker_${Date.now()}_${Math.random().toString(36).substr(2, 4)}`;
    
    const info = {
      id: workerId,
      worker,
      type,
      status: 'idle',
      currentTask: null,
      stats: { processed: 0, failed: 0, avgTime: 0, totalTime: 0 },
      registeredAt: Date.now(),
    };
    
    if (!this.workers[type]) this.workers[type] = [];
    this.workers[type].push(info);
    this.workerRegistry.set(workerId, info);
    
    this.emit('worker.registered', { workerId, type });
    console.log(`[LoadBalancer] Registered ${type} worker: ${workerId}`);
    return workerId;
  }

  unregisterWorker(workerId) {
    const info = this.workerRegistry.get(workerId);
    if (!info) return false;
    
    const workers = this.workers[info.type];
    if (workers) {
      const index = workers.findIndex(w => w.id === workerId);
      if (index !== -1) workers.splice(index, 1);
    }
    
    this.workerRegistry.delete(workerId);
    this.emit('worker.unregistered', { workerId });
    return true;
  }

  getAvailableWorker(type) {
    const workers = this.workers[type] || [];
    // ✅ No health check - just check if idle
    const available = workers.filter(w => w.status === 'idle');
    
    if (available.length === 0) return null;
    
    const index = this.roundRobinIndex % available.length;
    this.roundRobinIndex++;
    return available[index];
  }

  getWorkerTypeForTask(task) {
    if (task.workerType) return task.workerType;
    switch (task.type) {
      case 'execute':
        if (task.payload && task.payload.workerType) {
          return task.payload.workerType;
        }
        return 'submitter';
      case 'start_submitting':
      case 'submit_job':
        return 'submitter';
      case 'route_job':
        return task.to || task.payload?.to || 'analyzer';
      default:
        return null;
    }
  }

  handleWorkerResponse(workerId, message) {
    this.emit('worker.response', { ...message, workerId });
    this.emit('worker.direct.response', workerId, message);
  }

  async processTasks() {
    if (this.processing) return;
    this.processing = true;
    console.log('[LoadBalancer] Started processing loop');
    
    while (this.isRunning) {
      try {
        const task = this.dequeue();
        if (!task) { await this.sleep(10); continue; }
        await this.processTask(task);
      } catch (error) {
        console.error('[LoadBalancer] Processing error:', error);
        await this.sleep(100);
      }
    }
    
    console.log('[LoadBalancer] Processing loop stopped');
  }

  async processTask(task) {
    const workerType = this.getWorkerTypeForTask(task);
    
    if (!workerType) {
      this.enqueue(task, task.priority);
      await this.sleep(50);
      return;
    }
    
    const worker = this.getAvailableWorker(workerType);
    
    if (!worker) {
      this.enqueue(task, task.priority);
      await this.sleep(50);
      return;
    }
    
    await this.executeTask(worker, task);
  }

  async executeTask(worker, task) {
    worker.status = 'busy';
    worker.currentTask = task;
    task.attempts++;
    
    this.emit('task.assigned', { taskId: task.id, workerId: worker.id, workerType: worker.type });
    
    try {
      const startTime = Date.now();
      
      worker.worker.postMessage({
        type: 'execute',
        taskId: task.id,
        payload: task.payload || task,
      });
      
      const result = await this.waitForWorkerResponse(worker.id, task.id);
      const duration = Date.now() - startTime;
      
      worker.stats.processed++;
      worker.stats.totalTime += duration;
      worker.stats.avgTime = worker.stats.totalTime / worker.stats.processed;
      this.stats.tasksProcessed++;
      
      this.emit('task.complete', { taskId: task.id, workerId: worker.id, result, duration });
      
      if (result && result.requiresRouting && result.nextStage) {
        this.enqueue({
          type: 'route_job',
          workerType: result.nextStage,
          payload: { job: result.job || result, from: result.from, to: result.nextStage },
          priority: 'high',
        });
        this.stats.tasksRouted++;
      }
      
    } catch (error) {
      worker.stats.failed++;
      this.stats.tasksFailed++;
      
      this.emit('task.failed', { taskId: task.id, workerId: worker.id, error: error.message });
      
      if (task.attempts < task.maxAttempts) {
        this.enqueue(task, 'high');
      } else {
        this.emit('task.deadletter', { taskId: task.id, task, error: error.message });
      }
      
    } finally {
      worker.status = 'idle';
      worker.currentTask = null;
    }
  }

  waitForWorkerResponse(workerId, taskId) {
    return new Promise((resolve, reject) => {
      let resolved = false;
      
      const timeout = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          reject(new Error(`Worker ${workerId} timed out on task ${taskId}`));
        }
      }, this.options.workerTimeout);
      
      const responseHandler = (message) => {
        if (message.taskId !== taskId) return;
        if (message.type === 'task.complete') {
          resolved = true;
          clearTimeout(timeout);
          this.removeListener('worker.response', responseHandler);
          resolve(message.result);
        } else if (message.type === 'task.failed') {
          resolved = true;
          clearTimeout(timeout);
          this.removeListener('worker.response', responseHandler);
          reject(new Error(message.error || 'Task failed'));
        }
      };
      
      const directHandler = (id, message) => {
        if (id !== workerId) return;
        if (message.taskId !== taskId) return;
        if (message.type === 'task.complete') {
          resolved = true;
          clearTimeout(timeout);
          this.removeListener('worker.direct.response', directHandler);
          resolve(message.result);
        } else if (message.type === 'task.failed') {
          resolved = true;
          clearTimeout(timeout);
          this.removeListener('worker.direct.response', directHandler);
          reject(new Error(message.error || 'Task failed'));
        }
      };
      
      this.on('worker.response', responseHandler);
      this.on('worker.direct.response', directHandler);
    });
  }

  startProcessing() { this.processTasks(); }
  getQueueSize() { return this.queues.high.length + this.queues.normal.length + this.queues.low.length; }

  getWorkerStats() {
    const stats = {};
    for (const type of Object.keys(this.workers)) {
      stats[type] = {
        total: this.workers[type].length,
        idle: this.workers[type].filter(w => w.status === 'idle').length,
        busy: this.workers[type].filter(w => w.status === 'busy').length,
      };
    }
    return stats;
  }

  getStats() {
    return {
      queues: {
        high: this.queues.high.length,
        normal: this.queues.normal.length,
        low: this.queues.low.length,
        total: this.getQueueSize(),
      },
      workers: this.getWorkerStats(),
      stats: this.stats,
      registry: this.workerRegistry.size,
      isRunning: this.isRunning,
    };
  }

  sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
  generateId() { return `task_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`; }

  shutdown() {
    console.log('[LoadBalancer] Shutting down...');
    this.isRunning = false;
    this.processing = false;
    this.queues = { high: [], normal: [], low: [] };
    this.workerRegistry.clear();
    this.removeAllListeners();
    console.log('[LoadBalancer] Shutdown complete');
  }
}

module.exports = LoadBalancer;