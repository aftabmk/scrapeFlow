// workers/worker-pool.js
const { Worker } = require('worker_threads');
const { EventEmitter } = require('events');
const path = require('path');
const { namedMutex } = require('../utils/mutex');

class WorkerPool extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            minWorkers: options.minWorkers || 1,
            maxWorkers: options.maxWorkers || 4,
            loadBalancer: options.loadBalancer,
            eventBus: options.eventBus,
            orchestrator: options.orchestrator || null,
            ...options,
        };
        
        this.workers = new Map();
        this.workerAssignments = new Map();
        this.forwardingJobs = new Set();
        this.processingBatches = new Set();
        
        this.workerTypes = {
            sqlite: {
                script: path.join(__dirname, '../sqlite/worker.js'),
                count: 1,
                description: 'SQLite Database Worker'
            },
            puppeteer: {
                script: path.join(__dirname, '../puppeteer-server/worker.js'),
                count: 1,
                description: 'Puppeteer Browser Worker'
            },
            submitter: {
                script: path.join(__dirname, 'submitter-worker.js'),
                count: 1,
                description: 'Submits jobs'
            },
            analyzer: {
                script: path.join(__dirname, 'analyzer-worker.js'),
                count: 2,
                description: 'Analyzes data'
            },
            browser: {
                script: path.join(__dirname, 'browser-worker.js'),
                count: 1,
                description: 'Scrapes data'
            },
            exporter: {
                script: path.join(__dirname, 'exporter-worker.js'),
                count: 1,
                description: 'Exports data'
            },
        };
        
        this.stats = {
            created: 0,
            destroyed: 0,
            restarted: 0,
            errors: 0,
        };
        
        this._shuttingDown = false;
        this._shutdownComplete = false;
        
        console.log('[WorkerPool] Initialized');
    }

    async start() {
        if (this._shuttingDown || this._shutdownComplete) {
            console.log('[WorkerPool] Cannot start: already shutting down');
            return this;
        }
        
        console.log('[WorkerPool] Starting...');
        
        for (const [type, config] of Object.entries(this.workerTypes)) {
            for (let i = 0; i < config.count; i++) {
                await this.createWorker(type);
            }
        }
        
        console.log(`[WorkerPool] Started with ${this.workers.size} workers`);
        return this;
    }

    async createWorker(type) {
        if (this._shuttingDown || this._shutdownComplete) return null;
        
        const config = this.workerTypes[type];
        if (!config) {
            console.error(`[WorkerPool] Unknown worker type: ${type}`);
            return null;
        }
        
        const workerPath = config.script;
        const workerId = `worker_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
        
        try {
            const fs = require('fs');
            if (!fs.existsSync(workerPath)) {
                console.error(`[WorkerPool] Worker script not found: ${workerPath}`);
                return null;
            }
            
            const worker = new Worker(workerPath, {
                workerData: {
                    type,
                    id: workerId,
                    dbPath: process.env.DB_PATH || './data/queue.db',
                    readWorkers: parseInt(process.env.READ_WORKERS) || 2,
                    writeWorkers: parseInt(process.env.WRITE_WORKERS) || 2,
                    batchSize: parseInt(process.env.DB_BATCH_SIZE) || 50,
                    cacheSize: parseInt(process.env.DB_CACHE_SIZE) || 2000,
                    tabCount: parseInt(process.env.PUPPETEER_TABS) || 5,
                    headless: process.env.PUPPETEER_HEADLESS !== 'false',
                    devtools: process.env.PUPPETEER_DEVTOOLS === 'true',
                    timeout: parseInt(process.env.PUPPETEER_TIMEOUT) || 30000,
                }
            });
            
            const info = {
                id: workerId,
                worker,
                type,
                status: 'idle',
                created: Date.now(),
                processed: 0,
                errors: 0,
                currentTask: null,
            };
            
            this.workers.set(workerId, info);
            this.stats.created++;
            
            if (this.options.loadBalancer) {
                this.options.loadBalancer.registerWorker(worker, type);
            }
            
            worker.on('message', (message) => this.handleWorkerMessage(workerId, message));
            worker.on('error', (error) => this.handleWorkerError(workerId, error));
            worker.on('exit', (code) => this.handleWorkerExit(workerId, code));
            
            console.log(`[WorkerPool] Created ${type} worker: ${workerId}`);
            return info;
            
        } catch (error) {
            console.error(`[WorkerPool] Failed to create worker ${type}:`, error.message);
            return null;
        }
    }

    handleWorkerMessage(workerId, message) {
        const info = this.workers.get(workerId);
        if (!info) return;
        
        // Forward to orchestrator
        if (this.options.orchestrator) {
            this.options.orchestrator.handleWorkerMessage(workerId, message);
        }
        
        this.emit('worker.message', workerId, message);
        
        switch (message.type) {
            case 'task.complete':
                info.processed++;
                info.status = 'idle';
                info.currentTask = null;
                
                // Release worker assignment
                if (message.result && message.result.jobId) {
                    this.releaseWorker(workerId, message.result.jobId);
                }
                
                if (this.options.loadBalancer) {
                    this.options.loadBalancer.handleWorkerResponse(workerId, {
                        ...message,
                        workerId,
                        type: 'task.complete'
                    });
                }
                break;
                
            case 'task.failed':
                info.errors++;
                info.status = 'idle';
                info.currentTask = null;
                
                if (message.result && message.result.jobId) {
                    this.releaseWorker(workerId, message.result.jobId);
                }
                
                if (this.options.loadBalancer) {
                    this.options.loadBalancer.handleWorkerResponse(workerId, {
                        ...message,
                        workerId,
                        type: 'task.failed'
                    });
                }
                break;
                
            case 'worker.ready':
                info.status = 'idle';
                console.log(`[WorkerPool] Worker ${workerId} (${info.type}) ready`);
                break;
                
            case 'worker.shutdown':
                console.log(`[WorkerPool] Worker ${workerId} shutting down`);
                this.workers.delete(workerId);
                break;
                
            // ✅ System messages - broadcast
            case 'SQLITE_READY':
                console.log(`[WorkerPool] ✅ SQLite ready: ${workerId}`);
                this.broadcastToWorkers(message);
                this.emit('sqlite.ready', message);
                break;
                
            case 'SQLITE_RESPONSE':
                this.broadcastToWorkers(message);
                break;
                
            case 'SQLITE_ERROR':
                console.error(`[WorkerPool] ❌ SQLite error:`, message.error);
                this.broadcastToWorkers(message);
                break;
            
            case 'PUPPETEER_READY':
                console.log(`[WorkerPool] ✅ Puppeteer ready: ${workerId}`);
                this.broadcastToWorkers(message);
                this.emit('puppeteer.ready', message);
                break;
                
            case 'PUPPETEER_ERROR':
                console.error(`[WorkerPool] ❌ Puppeteer error:`, message.error);
                this.broadcastToWorkers(message);
                break;
            
            // ✅ SCRAPE_REQUEST - forward with mutex
            case 'SCRAPE_REQUEST':
                this.handleScrapeRequest(workerId, message);
                break;
            
            // ✅ SCRAPE_RESPONSE - handled by orchestrator
            case 'SCRAPE_RESPONSE':
                // Already handled by orchestrator
                break;
                
            case 'submitter.started':
                console.log(`[WorkerPool] Submitter started: ${message.payload.totalJobs} jobs`);
                break;
                
            case 'submitter.complete':
                console.log(`[WorkerPool] Submitter complete: ${message.payload.totalJobs} jobs`);
                break;
                
            case 'job.complete':
                console.log(`[WorkerPool] ✅ Job complete: ${message.payload.jobId}`);
                this.emit('job.complete', message.payload);
                break;
                
            case 'job.failed':
                console.error(`[WorkerPool] ❌ Job failed: ${message.payload.jobId}`);
                this.emit('job.failed', message.payload);
                break;
                
            default:
                break;
        }
    }

    /**
     * ✅ Handle scrape request with mutex to prevent duplicates
     */
    handleScrapeRequest(workerId, message) {
        const { messageId, payload, batchId } = message;
        const jobId = payload?.jobId;
        
        // ✅ Check if batch is already being processed
        if (batchId && this.processingBatches.has(batchId)) {
            console.log(`[WorkerPool] ⚠️ Batch ${batchId} already processing, ignoring duplicate`);
            this.sendDuplicateResponse(workerId, messageId, jobId);
            return;
        }
        
        // ✅ Use mutex for forwarding
        namedMutex.execute(`forward_scrape_${jobId}`, async () => {
            // ✅ Check if this job is already being forwarded
            if (jobId && this.forwardingJobs.has(jobId)) {
                console.log(`[WorkerPool] ⚠️ Job ${jobId} already being forwarded, skipping duplicate`);
                this.sendDuplicateResponse(workerId, messageId, jobId);
                return;
            }
            
            // ✅ Mark as forwarding
            if (jobId) {
                this.forwardingJobs.add(jobId);
            }
            if (batchId) {
                this.processingBatches.add(batchId);
            }
            
            try {
                // Forward to puppeteer worker
                const puppeteerWorker = this.getWorker('puppeteer');
                if (puppeteerWorker) {
                    puppeteerWorker.worker.postMessage(message);
                    console.log(`[WorkerPool] ✅ Forwarded SCRAPE_REQUEST for ${jobId} to puppeteer`);
                } else {
                    console.error(`[WorkerPool] ❌ No puppeteer worker available`);
                    this.sendErrorResponse(workerId, messageId, jobId, 'No puppeteer worker available');
                }
            } finally {
                // Release forwarding lock after a delay (to allow processing)
                setTimeout(() => {
                    if (jobId) {
                        this.forwardingJobs.delete(jobId);
                    }
                    if (batchId) {
                        this.processingBatches.delete(batchId);
                    }
                    console.log(`[WorkerPool] ✅ Released forwarding locks for ${jobId}`);
                }, 1000);
            }
        });
    }

    /**
     * Send duplicate response back to worker
     */
    sendDuplicateResponse(workerId, messageId, jobId) {
        const info = this.workers.get(workerId);
        if (info && info.worker) {
            info.worker.postMessage({
                type: 'SCRAPE_RESPONSE',
                messageId: messageId,
                payload: {
                    jobId: jobId,
                    duplicate: true,
                    error: 'Already processing',
                    success: false,
                    timestamp: Date.now()
                }
            });
        }
    }

    /**
     * Send error response back to worker
     */
    sendErrorResponse(workerId, messageId, jobId, error) {
        const info = this.workers.get(workerId);
        if (info && info.worker) {
            info.worker.postMessage({
                type: 'SCRAPE_RESPONSE',
                messageId: messageId,
                payload: {
                    jobId: jobId,
                    error: error,
                    success: false,
                    timestamp: Date.now()
                }
            });
        }
    }

    /**
     * ✅ Get worker with mutex
     */
    getWorker(type, jobId) {
        const workers = Array.from(this.workers.values());
        const available = workers.filter(w => w.type === type && w.status === 'idle');
        
        if (available.length === 0) {
            console.log(`[WorkerPool] ⚠️ No available ${type} workers`);
            return null;
        }
        
        // ✅ Check if this job already has a worker assigned
        if (jobId) {
            const assignedWorkerId = this.workerAssignments.get(jobId);
            if (assignedWorkerId) {
                const assignedWorker = this.workers.get(assignedWorkerId);
                if (assignedWorker && assignedWorker.status === 'idle') {
                    console.log(`[WorkerPool] ✅ Job ${jobId} already assigned to ${assignedWorkerId}`);
                    return assignedWorker;
                } else {
                    this.workerAssignments.delete(jobId);
                }
            }
        }
        
        // Round-robin selection
        const worker = available[0];
        worker.status = 'busy';
        
        if (jobId) {
            this.workerAssignments.set(jobId, worker.id);
        }
        
        console.log(`[WorkerPool] ✅ Assigned ${worker.id} (${type}) to job ${jobId || 'unknown'}`);
        return worker;
    }

    /**
     * ✅ Release worker assignment
     */
    releaseWorker(workerId, jobId) {
        const info = this.workers.get(workerId);
        if (info) {
            info.status = 'idle';
            info.currentTask = null;
        }
        
        if (jobId) {
            this.workerAssignments.delete(jobId);
            console.log(`[WorkerPool] ✅ Released worker ${workerId} from job ${jobId}`);
        }
    }

    /**
     * ✅ Forward message to worker by type
     */
    forwardToWorker(type, message) {
        for (const [id, info] of this.workers) {
            if (info.type === type) {
                info.worker.postMessage(message);
                console.log(`[WorkerPool] ✅ Forwarded to ${type} worker: ${id}`);
                return;
            }
        }
        console.error(`[WorkerPool] ❌ No worker found for type: ${type}`);
    }

    /**
     * ✅ Send message to specific worker
     */
    sendToWorker(workerId, message) {
        const info = this.workers.get(workerId);
        if (info && info.worker) {
            info.worker.postMessage(message);
            console.log(`[WorkerPool] ✅ Sent to worker: ${workerId}`);
            return;
        }
        console.error(`[WorkerPool] ❌ Worker not found: ${workerId}`);
    }

    /**
     * ✅ Broadcast message to all workers
     */
    broadcastToWorkers(message) {
        let count = 0;
        for (const [id, info] of this.workers) {
            try {
                info.worker.postMessage(message);
                count++;
            } catch (err) {
                // Ignore
            }
        }
        console.log(`[WorkerPool] ✅ Broadcasted to ${count} workers`);
    }

    handleWorkerError(workerId, error) {
        const info = this.workers.get(workerId);
        if (!info) return;
        
        this.stats.errors++;
        console.error(`[WorkerPool] Worker ${workerId} (${info.type}) error:`, error.message);
        this.emit('worker.error', { workerId, error: error.message });
        
        if (!this._shuttingDown) {
            this.restartWorker(workerId);
        }
    }

    handleWorkerExit(workerId, code) {
        const info = this.workers.get(workerId);
        if (!info) return;
        
        console.log(`[WorkerPool] Worker ${workerId} (${info.type}) exited with code ${code}`);
        this.stats.destroyed++;
        this.emit('worker.exited', { workerId, code });
        
        if (this.options.loadBalancer) {
            this.options.loadBalancer.unregisterWorker(workerId);
        }
        
        if (!this._shuttingDown) {
            console.log(`[WorkerPool] Restarting worker ${workerId} (${info.type})`);
            this.restartWorker(workerId);
        } else {
            this.workers.delete(workerId);
        }
    }

    async restartWorker(workerId, type = null) {
        if (this._shuttingDown || this._shutdownComplete) {
            console.log('[WorkerPool] Skipping restart: shutting down');
            return null;
        }
        
        const info = this.workers.get(workerId);
        if (info) {
            type = info.type;
        }
        
        if (!type) {
            console.warn(`[WorkerPool] Cannot restart worker ${workerId}: type unknown`);
            return null;
        }
        
        console.log(`[WorkerPool] Restarting worker ${workerId} (${type})`);
        
        if (this.workers.has(workerId)) {
            const oldInfo = this.workers.get(workerId);
            if (oldInfo && oldInfo.worker) {
                try {
                    await oldInfo.worker.terminate();
                } catch (err) {}
            }
            this.workers.delete(workerId);
        }
        
        if (this.options.loadBalancer) {
            this.options.loadBalancer.unregisterWorker(workerId);
        }
        
        this.stats.restarted++;
        await this.sleep(1000);
        
        if (this._shuttingDown || this._shutdownComplete) return null;
        
        const newWorker = await this.createWorker(type);
        if (newWorker) {
            this.emit('worker.restarted', { oldId: workerId, newId: newWorker.id });
            console.log(`[WorkerPool] Worker ${workerId} restarted as ${newWorker.id}`);
        } else {
            console.error(`[WorkerPool] Failed to restart worker ${workerId}`);
            this.emit('worker.restart.failed', { workerId });
        }
        
        return newWorker;
    }

    getStats() {
        const workers = Array.from(this.workers.values());
        const byType = {};
        
        for (const type of Object.keys(this.workerTypes)) {
            const list = workers.filter(w => w.type === type);
            byType[type] = {
                total: list.length,
                idle: list.filter(w => w.status === 'idle').length,
                busy: list.filter(w => w.status === 'busy').length,
            };
        }
        
        return {
            total: workers.length,
            byType,
            stats: this.stats,
            shuttingDown: this._shuttingDown,
            forwardingJobs: this.forwardingJobs.size,
            processingBatches: this.processingBatches.size,
            workerAssignments: this.workerAssignments.size,
        };
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async shutdown() {
        if (this._shuttingDown) {
            console.log('[WorkerPool] Already shutting down...');
            return;
        }
        
        if (this._shutdownComplete) {
            console.log('[WorkerPool] Shutdown already complete');
            return;
        }
        
        this._shuttingDown = true;
        console.log('[WorkerPool] Shutting down...');
        
        // Clear all tracking
        this.workerAssignments.clear();
        this.forwardingJobs.clear();
        this.processingBatches.clear();
        
        const promises = [];
        for (const [id, info] of this.workers) {
            promises.push(
                info.worker.terminate().catch(err => {
                    console.error(`[WorkerPool] Error terminating ${id}:`, err.message);
                })
            );
        }
        
        await Promise.allSettled(promises);
        this.workers.clear();
        
        this._shutdownComplete = true;
        console.log('[WorkerPool] Shutdown complete');
        this.emit('shutdown');
    }
}

module.exports = WorkerPool;