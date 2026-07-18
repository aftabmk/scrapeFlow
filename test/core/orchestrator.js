// core/orchestrator.js
const { EventEmitter } = require('events');
const EventBus = require('./event-bus');
const LoadBalancer = require('./load-balancer');
const QueueManager = require('./queue-manager');
const StateManager = require('./state-manager');
const WorkerPool = require('../workers/worker-pool');

class Orchestrator extends EventEmitter {
  constructor(config = {}) {
    super();
    
    this.config = config;
    this.components = {};
    this.isRunning = false;
    this.sqliteReady = false;
    this.puppeteerReady = false;
    this.memoryMode = true;
    this.startTime = null;
    this._shuttingDown = false;
    this._shutdownComplete = false;
    
    // ✅ Pending requests map for orchestrator-managed communication
    this.pendingRequests = new Map();
    
    this.initComponents();
    this.setupEventListeners();
    this.setupWorkerCommunication();
    
    console.log('[Orchestrator] Initialized');
    console.log(`[Orchestrator] Mode: Memory-first (SQLite & Puppeteer as workers)`);
  }

  initComponents() {
    this.components.eventBus = new EventBus();
    this.components.stateManager = new StateManager({ 
      checkpointInterval: this.config.health?.heartbeatInterval || 5000 
    });
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
    
    // ✅ WorkerPool manages ALL workers with orchestrator reference
    this.components.workerPool = new WorkerPool({
      minWorkers: this.config.workers?.min || 4,
      maxWorkers: this.config.workers?.max || 16,
      loadBalancer: this.components.loadBalancer,
      eventBus: this.components.eventBus,
      orchestrator: this,  // ✅ Pass orchestrator reference
    });
  }

  /**
   * ✅ Setup worker communication (Orchestrator handles inter-worker messages)
   */
  setupWorkerCommunication() {
    // Listen for worker messages via WorkerPool
    this.components.workerPool.on('worker.message', (workerId, message) => {
      this.handleWorkerMessage(workerId, message);
    });
  }

  /**
   * ✅ Handle all worker messages centrally
   */
  handleWorkerMessage(workerId, message) {
    if (!message || !message.type) return;
    
    switch (message.type) {
      case 'SCRAPE_REQUEST':
        this.handleScrapeRequest(workerId, message);
        break;
        
      case 'SCRAPE_RESPONSE':
        this.handleScrapeResponse(workerId, message);
        break;
        
      case 'PUPPETEER_READY':
        this.puppeteerReady = true;
        console.log('[Orchestrator] ✅ Puppeteer ready');
        this.components.stateManager.update('health.puppeteer', 'healthy');
        this.emit('puppeteer.ready', message);
        break;
        
      case 'PUPPETEER_ERROR':
        console.error('[Orchestrator] ❌ Puppeteer error:', message.error);
        this.puppeteerReady = false;
        this.components.stateManager.update('health.puppeteer', 'unhealthy');
        this.emit('puppeteer.error', message);
        break;
        
      default:
        // Ignore other messages (handled elsewhere)
        break;
    }
  }

  /**
   * ✅ Handle scrape request from browser worker
   */
  handleScrapeRequest(workerId, message) {
    const { messageId, payload, sourceWorkerId } = message;
    const { jobId, url } = payload || {};
    const targetWorkerId = sourceWorkerId || workerId;
    
    console.log(`[Orchestrator] 📥 Received SCRAPE_REQUEST from ${targetWorkerId} for ${jobId}`);
    
    // ✅ Store pending request with timeout
    const timeout = setTimeout(() => {
      if (this.pendingRequests.has(messageId)) {
        this.pendingRequests.delete(messageId);
        console.error(`[Orchestrator] ⏰ Timeout for ${messageId} (${jobId})`);
        
        // Send timeout response back to browser worker
        this.components.workerPool.sendToWorker(targetWorkerId, {
          type: 'SCRAPE_RESPONSE',
          messageId: messageId,
          payload: {
            jobId: jobId,
            error: 'Timeout waiting for puppeteer response',
            success: false,
            timestamp: Date.now()
          }
        });
      }
    }, 30000);
    
    this.pendingRequests.set(messageId, {
      workerId: targetWorkerId,
      timeout: timeout,
      timestamp: Date.now()
    });
    
    // ✅ Forward to puppeteer worker
    this.components.workerPool.forwardToWorker('puppeteer', {
      ...message,
      sourceWorkerId: targetWorkerId,
    });
    
    console.log(`[Orchestrator] 📤 Forwarded SCRAPE_REQUEST to puppeteer for ${jobId}`);
  }

  /**
   * ✅ Handle scrape response from puppeteer worker
   */
  handleScrapeResponse(workerId, message) {
    const { messageId, payload } = message;
    
    console.log(`[Orchestrator] 📥 Received SCRAPE_RESPONSE for ${messageId}`);
    
    const pending = this.pendingRequests.get(messageId);
    if (pending) {
      // ✅ Clear timeout
      clearTimeout(pending.timeout);
      this.pendingRequests.delete(messageId);
      
      // ✅ Send response back to the original browser worker
      this.components.workerPool.sendToWorker(pending.workerId, {
        type: 'SCRAPE_RESPONSE',
        messageId: messageId,
        payload: payload
      });
      
      console.log(`[Orchestrator] 📤 Sent SCRAPE_RESPONSE back to browser worker ${pending.workerId}`);
    } else {
      console.warn(`[Orchestrator] ⚠️ No pending request for ${messageId}`);
    }
  }

  setupEventListeners() {
    const { eventBus, queueManager, stateManager, workerPool } = this.components;
    
    // SQLite events from worker
    workerPool.on('sqlite.ready', (event) => {
      console.log('[Orchestrator] SQLite ready - enabling persistence');
      this.sqliteReady = true;
      this.memoryMode = false;
      queueManager.setPersistence(this.components.sqlite);
      stateManager.update('system.status', 'running');
      stateManager.update('health.sqlite', 'healthy');
      this.emit('sqlite.ready', event);
    });
    
    // Job events
    eventBus.subscribe('job.submitted', async (event) => {
      const { job, queue } = event;
      queueManager.enqueue(queue || 'submitter', job);
      stateManager.updateJob(job.id, { 
        status: 'pending', 
        queue: queue || 'submitter' 
      });
      this.emit('job.submitted', event);
    });
    
    eventBus.subscribe('job.queued', (event) => {
      stateManager.updateJob(event.jobId, { 
        status: 'queued', 
        queue: event.queue 
      });
      this.emit('job.queued', event);
    });
    
    eventBus.subscribe('job.dequeued', (event) => {
      stateManager.updateJob(event.jobId, { 
        status: 'processing', 
        workerId: event.workerId 
      });
      this.emit('job.dequeued', event);
    });
    
    eventBus.subscribe('job.acked', (event) => {
      stateManager.updateJob(event.jobId, { status: 'completed' });
      this.emit('job.acked', event);
    });
    
    eventBus.subscribe('job.failed', (event) => {
      stateManager.updateJob(event.jobId, { 
        status: 'failed', 
        error: event.error 
      });
      this.emit('job.failed', event);
    });
    
    eventBus.subscribe('job.deadletter', (event) => {
      stateManager.updateJob(event.jobId, { status: 'deadletter' });
      this.emit('job.deadletter', event);
    });
    
    // Worker events
    eventBus.subscribe('worker.registered', (event) => {
      stateManager.updateWorker(event.workerId, {
        type: event.type,
        status: 'registered',
        registeredAt: Date.now(),
      });
      this.emit('worker.registered', event);
    });
    
    // Pipeline events
    eventBus.subscribe('pipeline.started', (event) => {
      stateManager.update('pipeline.status', 'running');
      this.emit('pipeline.started', event);
    });
    
    eventBus.subscribe('pipeline.completed', (event) => {
      stateManager.update('pipeline.status', 'completed');
      this.emit('pipeline.completed', event);
    });
    
    // Health events
    eventBus.subscribe('health.check', () => {
      this.checkHealth();
    });
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
    
    // ✅ Start ALL workers (including SQLite and Puppeteer)
    await Promise.all([
      this.components.workerPool.start(),
      this.components.loadBalancer.startProcessing(),
    ]);
    
    this.components.eventBus.publish('system.ready', { 
      timestamp: Date.now(), 
      version: this.config.app?.version || '3.0.0' 
    });
    
    this.components.stateManager.update('system.status', 'ready');
    this.isRunning = true;
    
    console.log('[Orchestrator] Started (SQLite & Puppeteer initializing as workers)');
    console.log('[Orchestrator] Jobs can be processed immediately!');
    
    if (events.length > 0) {
      await this.startPipeline(events);
    }
    
    return this;
  }

  async startPipeline(events) {
    if (this._shuttingDown || this._shutdownComplete) return;
    
    console.log(`[Orchestrator] 🚀 Starting pipeline with ${events.length} events`);
    this.components.eventBus.publish('pipeline.started', {
      events: events.length,
      timestamp: Date.now()
    });
    
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
          metadata: {
            exchange: event.EXCHANGE,
            contract: event.CONTRACT,
          },
        });
      }
    }
    
    this.emit('pipeline.started', { events });
  }

  async submitJob(job) {
    if (this._shuttingDown || this._shutdownComplete) return null;
    
    const { queueManager, stateManager, loadBalancer } = this.components;
    
    const jobId = job.id || `job_${Date.now()}`;
    const jobData = {
      id: jobId,
      data: job.data || job,
      metadata: job.metadata || {},
      status: 'pending',
      submittedAt: Date.now()
    };
    
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
    if (taskId) {
      console.log(`[Orchestrator] 📤 Enqueued submit_job task: ${taskId} for ${jobId}`);
    }
    
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
      puppeteerReady: this.puppeteerReady,
      pendingRequests: this.pendingRequests.size,
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
        puppeteer: this.puppeteerReady ? 'healthy' : 'starting',
        queueManager: 'healthy',
        loadBalancer: loadBalancer.isRunning ? 'healthy' : 'unhealthy',
        workerPool: 'healthy',
      },
      loadBalancerStats: loadBalancer.getStats(),
      pendingRequests: this.pendingRequests.size,
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
      pendingRequests: this.pendingRequests.size,
      memoryMode: this.memoryMode,
      sqliteReady: this.sqliteReady,
      puppeteerReady: this.puppeteerReady,
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
      // ✅ Clear all pending requests
      for (const [messageId, pending] of this.pendingRequests) {
        clearTimeout(pending.timeout);
        console.log(`[Orchestrator] Clearing pending request: ${messageId}`);
      }
      this.pendingRequests.clear();
      
      const safeShutdown = async (component, name) => {
        if (component && typeof component.shutdown === 'function') {
          try {
            await component.shutdown();
            console.log(`[Orchestrator] ${name} shutdown complete`);
          } catch (error) {
            console.error(`[Orchestrator] ${name} shutdown error:`, error.message);
          }
        } else if (component) {
          console.log(`[Orchestrator] ${name} has no shutdown method, skipping`);
        } else {
          console.log(`[Orchestrator] ${name} not initialized, skipping`);
        }
      };

      this.isRunning = false;
      await safeShutdown(this.components.workerPool, 'WorkerPool');
      await safeShutdown(this.components.loadBalancer, 'LoadBalancer');
      await safeShutdown(this.components.queueManager, 'QueueManager');
      await safeShutdown(this.components.stateManager, 'StateManager');
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