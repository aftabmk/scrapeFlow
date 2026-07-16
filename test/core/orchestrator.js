// core/orchestrator.js
const { EventEmitter } = require('events');
const EventBus = require('./event-bus');
const LoadBalancer = require('./load-balancer');
const QueueManager = require('./queue-manager');
const StateManager = require('./state-manager');
const WorkerPool = require('../workers/worker-pool');
const SQLiteServer = require('../sqlite/server');

class Orchestrator extends EventEmitter {
  constructor(config = {}) {
    super();
    
    this.config = config;
    this.components = {};
    this.isRunning = false;
    this.sqliteReady = false;
    this.memoryMode = true;
    this.startTime = null;
    this._shuttingDown = false;
    this._shutdownComplete = false;
    
    this.initComponents();
    this.setupEventListeners();
    
    console.log('[Orchestrator] Initialized (Health checks disabled)');
    console.log(`[Orchestrator] Mode: Memory-first (SQLite lazy)`);
  }

  initComponents() {
    this.components.eventBus = new EventBus();
    this.components.stateManager = new StateManager({ checkpointInterval: this.config.health?.heartbeatInterval || 5000 });
    this.components.queueManager = new QueueManager({
      visibilityTimeout: this.config.queues?.visibilityTimeout || 30000,
      maxRetries: this.config.queues?.maxRetries || 3,
      batchSize: this.config.queues?.batchSize || 10,
    });
    this.components.loadBalancer = new LoadBalancer({
      maxQueueSize: this.config.queues?.maxQueueSize || 10000,
      workerTimeout: this.config.health?.heartbeatTimeout || 30000,
      batchSize: this.config.queues?.batchSize || 50,
    });
    this.components.workerPool = new WorkerPool({
      minWorkers: this.config.workers?.min || 4,
      maxWorkers: this.config.workers?.max || 16,
      loadBalancer: this.components.loadBalancer,
      eventBus: this.components.eventBus,
    });
    this.components.sqlite = new SQLiteServer({
      dbPath: this.config.database?.path || './data/queue.db',
      readWorkers: this.config.database?.readWorkers || 2,
      writeWorkers: this.config.database?.writeWorkers || 2,
      batchSize: this.config.database?.batchSize || 50,
      cacheSize: this.config.database?.cacheSize || 2000,
    });
  }

  setupEventListeners() {
    const { eventBus, queueManager, stateManager } = this.components;
    
    eventBus.subscribe('sqlite.ready', async (event) => {
      console.log('[Orchestrator] SQLite ready - enabling persistence');
      this.sqliteReady = true;
      this.memoryMode = false;
      queueManager.setPersistence(this.components.sqlite);
      stateManager.update('system.status', 'running');
      stateManager.update('health.sqlite', 'healthy');
      this.emit('sqlite.ready');
    });
    
    eventBus.subscribe('sqlite.error', (event) => {
      console.error('[Orchestrator] SQLite error:', event.error);
      stateManager.update('health.sqlite', 'unhealthy');
      this.memoryMode = true;
      this.emit('sqlite.error', event);
    });
    
    eventBus.subscribe('job.submitted', async (event) => {
      const { job, queue } = event;
      queueManager.enqueue(queue || 'submitter', job);
      stateManager.updateJob(job.id, { status: 'pending', queue: queue || 'submitter' });
      this.emit('job.submitted', event);
    });
    
    eventBus.subscribe('job.queued', (event) => {
      stateManager.updateJob(event.jobId, { status: 'queued', queue: event.queue });
      this.emit('job.queued', event);
    });
    
    eventBus.subscribe('job.dequeued', (event) => {
      stateManager.updateJob(event.jobId, { status: 'processing', workerId: event.workerId });
      this.emit('job.dequeued', event);
    });
    
    eventBus.subscribe('job.acked', (event) => {
      stateManager.updateJob(event.jobId, { status: 'completed' });
      this.emit('job.acked', event);
    });
    
    eventBus.subscribe('job.failed', (event) => {
      stateManager.updateJob(event.jobId, { status: 'failed', error: event.error });
      this.emit('job.failed', event);
    });
    
    eventBus.subscribe('job.deadletter', (event) => {
      stateManager.updateJob(event.jobId, { status: 'deadletter' });
      this.emit('job.deadletter', event);
    });
    
    eventBus.subscribe('worker.registered', (event) => {
      stateManager.updateWorker(event.workerId, { type: event.type, status: 'registered', registeredAt: Date.now() });
      this.emit('worker.registered', event);
    });
    
    eventBus.subscribe('pipeline.started', (event) => {
      stateManager.update('pipeline.status', 'running');
      this.emit('pipeline.started', event);
    });
    
    eventBus.subscribe('pipeline.completed', (event) => {
      stateManager.update('pipeline.status', 'completed');
      this.emit('pipeline.completed', event);
    });
    
    eventBus.subscribe('health.check', () => { this.checkHealth(); });
  }

  async start(events = []) {
    if (this._shuttingDown || this._shutdownComplete) {
      console.log('[Orchestrator] Cannot start: already shutting down or complete');
      return this;
    }
    
    this.isRunning = true;
    this.startTime = Date.now();
    console.log('[Orchestrator] Starting...');
    console.log(`[Orchestrator] Events: ${events.length}`);
    
    this.components.stateManager.update('system.status', 'starting');
    this.components.stateManager.update('system.startTime', this.startTime);
    
    await Promise.all([
      this.components.workerPool.start(),
      this.components.loadBalancer.startProcessing(),
    ]);
    
    this.startSQLiteBackground();
    
    this.components.eventBus.publish('system.ready', { timestamp: Date.now(), version: this.config.app?.version || '3.0.0' });
    this.components.stateManager.update('system.status', 'ready');
    this.isRunning = true;
    
    console.log('[Orchestrator] Started (SQLite initializing in background)');
    console.log('[Orchestrator] Jobs can be processed immediately!');
    
    if (events.length > 0) await this.startPipeline(events);
    return this;
  }

  async startSQLiteBackground() {
    try {
      console.log('[Orchestrator] Starting SQLite in background...');
      await this.components.sqlite.start();
      this.components.eventBus.publish('sqlite.ready', { timestamp: Date.now(), dbPath: this.config.database?.path });
      console.log('[Orchestrator] SQLite ready');
    } catch (error) {
      console.error('[Orchestrator] SQLite start error:', error.message);
      this.components.eventBus.publish('sqlite.error', { error: error.message, timestamp: Date.now() });
    }
  }

  async startPipeline(events) {
    if (this._shuttingDown || this._shutdownComplete) return;
    
    console.log(`[Orchestrator] 🚀 Starting pipeline with ${events.length} events`);
    this.components.eventBus.publish('pipeline.started', { events: events.length, timestamp: Date.now() });
    
    const task = {
      type: 'execute',
      workerType: 'submitter',
      payload: {
        type: 'start_submitting',
        events: events,
        maxJobs: events.length,
        interval: parseInt(process.env.SUBMIT_INTERVAL) || 1000,
        timestamp: Date.now(),
      },
      priority: 'high',
    };
    
    const taskId = this.components.loadBalancer.enqueue(task);
    
    if (taskId) {
      console.log(`[Orchestrator] 📤 Enqueued start_submitting task: ${taskId}`);
    } else {
      console.error(`[Orchestrator] ❌ Failed to enqueue start_submitting task`);
      for (const event of events) {
        await this.submitJob({
          id: `${event.EXCHANGE}-${event.CONTRACT}`,
          data: event,
          metadata: { exchange: event.EXCHANGE, contract: event.CONTRACT },
        });
      }
    }
    
    this.emit('pipeline.started', { events });
  }

  async submitJob(job) {
    if (this._shuttingDown || this._shutdownComplete) return null;
    const { queueManager, stateManager, loadBalancer } = this.components;
    
    const jobId = job.id || `job_${Date.now()}`;
    const jobData = { id: jobId, data: job.data || job, metadata: job.metadata || {}, status: 'pending', submittedAt: Date.now() };
    stateManager.updateJob(jobId, jobData);
    queueManager.enqueue('submitter', jobData);
    
    const task = {
      type: 'execute',
      workerType: 'submitter',
      payload: {
        type: 'submit_job',
        job: jobData,
        event: job.data,
        index: job.metadata?.index || 1,
        total: job.metadata?.total || 1,
      },
      priority: 'normal',
    };
    
    const taskId = loadBalancer.enqueue(task);
    if (taskId) console.log(`[Orchestrator] 📤 Enqueued submit_job task: ${taskId} for ${jobId}`);
    
    this.emit('job.submitted', { jobId, job: jobData });
    return jobId;
  }

  getStatus() {
    const { stateManager, queueManager, loadBalancer, workerPool } = this.components;
    return {
      system: stateManager.getSystemStatus(),
      health: stateManager.getHealth(),
      pipeline: stateManager.getPipelineStatus(),
      queues: queueManager.getStats(),
      loadBalancer: loadBalancer.getStats(),
      workers: workerPool.getStats(),
      memoryMode: this.memoryMode,
      sqliteReady: this.sqliteReady,
      uptime: Date.now() - this.startTime,
      shuttingDown: this._shuttingDown,
    };
  }

  checkHealth() {
    if (this._shuttingDown || this._shutdownComplete) return null;
    const { stateManager, loadBalancer } = this.components;
    const health = {
      system: stateManager.getSystemStatus(),
      components: {
        sqlite: this.sqliteReady ? 'healthy' : 'starting',
        queueManager: 'healthy',
        loadBalancer: loadBalancer.isRunning ? 'healthy' : 'unhealthy',
        workerPool: 'healthy',
      },
      loadBalancerStats: loadBalancer.getStats(),
      timestamp: Date.now(),
    };
    stateManager.update('health', health);
    this.components.eventBus.publish('health.report', health);
    this.emit('health.check', health);
    return health;
  }

  getStats() {
    const { stateManager, queueManager, loadBalancer, eventBus, workerPool } = this.components;
    return {
      state: stateManager.getStats(),
      queues: queueManager.getStats(),
      loadBalancer: loadBalancer.getStats(),
      workers: workerPool.getStats(),
      events: eventBus.getStats(),
      memoryMode: this.memoryMode,
      sqliteReady: this.sqliteReady,
      uptime: Date.now() - this.startTime,
    };
  }

  async shutdown() {
    if (this._shuttingDown) {
      console.log('[Orchestrator] Already shutting down...');
      await new Promise(resolve => setTimeout(resolve, 500));
      return;
    }
    if (this._shutdownComplete) {
      console.log('[Orchestrator] Shutdown already complete');
      return;
    }
    
    this._shuttingDown = true;
    console.log('[Orchestrator] Shutting down...');
    
    try {
      const safeShutdown = async (component, name) => {
        if (component && typeof component.shutdown === 'function') {
          try { await component.shutdown(); console.log(`[Orchestrator] ${name} shutdown complete`); } 
          catch (error) { console.error(`[Orchestrator] ${name} shutdown error:`, error.message); }
        } else if (component) { console.log(`[Orchestrator] ${name} has no shutdown method, skipping`); } 
        else { console.log(`[Orchestrator] ${name} not initialized, skipping`); }
      };

      this.isRunning = false;
      await safeShutdown(this.components.workerPool, 'WorkerPool');
      await safeShutdown(this.components.loadBalancer, 'LoadBalancer');
      await safeShutdown(this.components.queueManager, 'QueueManager');
      await safeShutdown(this.components.stateManager, 'StateManager');
      await safeShutdown(this.components.sqlite, 'SQLite');
      await safeShutdown(this.components.eventBus, 'EventBus');
      
      this._shutdownComplete = true;
      this._shuttingDown = false;
      console.log('[Orchestrator] Shutdown complete');
    } catch (error) {
      console.error('[Orchestrator] Shutdown error:', error);
      this._shuttingDown = false;
      throw error;
    }
    this.emit('shutdown');
  }
}

module.exports = Orchestrator;