// core/queue-manager.js
const { EventEmitter } = require('events');
const { namedMutex } = require('../utils/mutex');

class QueueManager extends EventEmitter {
    constructor(options = {}) {
        super();
        
        this.options = {
            visibilityTimeout: 30000,
            maxRetries: 3,
            batchSize: 10,
            maxQueueSize: 10000,
            sweepInterval: 1000,
            ...options,
        };
        
        // In-memory queues (fast access)
        this.queues = new Map();
        this.inFlight = new Map();
        this.deadLetter = [];
        this.completed = new Set();
        
        // Persistence
        this.persistence = null;
        this.persistenceReady = false;
        this.sqliteWorker = null;
        this.pendingWrites = [];
        this.requestId = 0;
        this.pendingRequests = new Map();
        
        // Stats
        this.stats = {
            enqueued: 0,
            dequeued: 0,
            completed: 0,
            failed: 0,
            timedOut: 0,
            deadLettered: 0,
        };
        
        this.startSweeper();
        this.startFlusher();
        console.log('[QueueManager] Initialized (Durable mode disabled)');
    }

    /**
     * ✅ Set SQLite persistence
     */
    setPersistence(sqliteWorker) {
        this.sqliteWorker = sqliteWorker;
        this.persistenceReady = true;
        console.log('[QueueManager] ✅ Persistence enabled');
        
        // Recover state from SQLite
        this.recoverState();
    }

    /**
     * ✅ Recover state from SQLite on startup
     */
    async recoverState() {
        if (!this.sqliteWorker) return;
        
        console.log('[QueueManager] 🔄 Recovering state from SQLite...');
        
        try {
            const result = await this.sendSQLiteRequest('recover_all', {});
            
            if (result && result.payload) {
                const recovered = result.payload;
                let totalRecovered = 0;
                
                for (const [queue, data] of Object.entries(recovered)) {
                    if (data && data.rows) {
                        const queueObj = this.getQueue(queue);
                        for (const row of data.rows) {
                            // Re-add jobs to queue
                            queueObj.pending.push(row);
                            totalRecovered++;
                        }
                    }
                }
                
                console.log(`[QueueManager] ✅ Recovered ${totalRecovered} jobs from SQLite`);
            }
            
            // Recover in-flight jobs (requeue them)
            const inFlightResult = await this.sendSQLiteRequest('get_in_flight', { queue: 'all' });
            if (inFlightResult && inFlightResult.payload) {
                let requeued = 0;
                for (const job of inFlightResult.payload) {
                    await this.requeue(job.queue, job.job_id);
                    requeued++;
                }
                if (requeued > 0) {
                    console.log(`[QueueManager] 🔄 Requeued ${requeued} in-flight jobs`);
                }
            }
            
        } catch (error) {
            console.error('[QueueManager] ❌ Recovery failed:', error.message);
        }
    }

    /**
     * ✅ Send request to SQLite worker
     */
    sendSQLiteRequest(operation, data) {
        return new Promise((resolve, reject) => {
            if (!this.sqliteWorker) {
                reject(new Error('SQLite worker not available'));
                return;
            }
            
            const requestId = `sqlite_${++this.requestId}`;
            
            this.pendingRequests.set(requestId, { resolve, reject });
            
            this.sqliteWorker.postMessage({
                type: 'SQLITE_REQUEST',
                requestId,
                payload: {
                    operation,
                    ...data
                }
            });
        });
    }

    /**
     * ✅ Handle SQLite response
     */
    handleSQLiteResponse(message) {
        const { requestId, payload } = message;
        const pending = this.pendingRequests.get(requestId);
        
        if (pending) {
            if (payload.error) {
                pending.reject(new Error(payload.error));
            } else {
                pending.resolve({ payload });
            }
            this.pendingRequests.delete(requestId);
        }
    }

    getQueue(name) {
        if (!this.queues.has(name)) {
            this.queues.set(name, {
                name,
                pending: [],
                processing: new Set(),
                completed: new Set(),
                failed: new Set(),
                stats: { enqueued: 0, dequeued: 0, completed: 0, failed: 0 },
            });
        }
        return this.queues.get(name);
    }

    /**
     * ✅ Enqueue with persistence
     */
    async enqueue(queueName, job, priority = 'normal') {
        const queue = this.getQueue(queueName);
        if (!job.id) job.id = this.generateJobId();
        
        const wrappedJob = {
            ...job,
            queue: queueName,
            priority,
            enqueuedAt: Date.now(),
            attempts: 0,
            maxAttempts: job.maxAttempts || this.options.maxRetries,
            status: 'pending',
            metadata: job.metadata || {},
        };
        
        if (queue.pending.length >= this.options.maxQueueSize) {
            this.emit('queue.full', { queueName, job: wrappedJob });
            return null;
        }
        
        // Add to memory
        if (priority === 'high') {
            queue.pending.unshift(wrappedJob);
        } else if (priority === 'low') {
            queue.pending.push(wrappedJob);
        } else {
            const insertAt = Math.floor(queue.pending.length / 3);
            queue.pending.splice(insertAt, 0, wrappedJob);
        }
        
        queue.stats.enqueued++;
        this.stats.enqueued++;
        
        // ✅ Persist to SQLite if ready
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('append', {
                    queue: queueName,
                    jobId: wrappedJob.id,
                    data: wrappedJob
                });
            } catch (error) {
                console.error('[QueueManager] ❌ Failed to persist enqueue:', error.message);
                // Keep in memory, but log error
            }
        }
        
        this.emit('job.enqueued', { queueName, job: wrappedJob });
        return wrappedJob.id;
    }

    /**
     * ✅ Dequeue with persistence
     */
    async dequeue(queueName, workerId) {
        const queue = this.getQueue(queueName);
        if (queue.pending.length === 0) return null;
        
        const job = queue.pending.shift();
        job.status = 'processing';
        job.dequeuedAt = Date.now();
        job.workerId = workerId;
        job.expiresAt = Date.now() + this.options.visibilityTimeout;
        
        // Track in-memory in-flight
        this.inFlight.set(job.id, { 
            job, 
            queue: queueName, 
            workerId, 
            expiresAt: job.expiresAt,
            retryCount: 0 
        });
        queue.processing.add(job.id);
        queue.stats.dequeued++;
        this.stats.dequeued++;
        
        // ✅ Persist to SQLite if ready
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('deliver', {
                    queue: queueName,
                    jobId: job.id
                });
            } catch (error) {
                console.error('[QueueManager] ❌ Failed to persist dequeue:', error.message);
            }
        }
        
        this.emit('job.dequeued', { queueName, jobId: job.id, workerId });
        return job;
    }

    /**
     * ✅ Acknowledge with persistence
     */
    async ack(queueName, jobId) {
        const queue = this.getQueue(queueName);
        const inFlight = this.inFlight.get(jobId);
        if (!inFlight) { 
            this.emit('ack.unknown', { queueName, jobId }); 
            return false; 
        }
        
        // Remove from memory
        this.inFlight.delete(jobId);
        queue.processing.delete(jobId);
        queue.completed.add(jobId);
        const job = inFlight.job;
        job.status = 'completed';
        job.completedAt = Date.now();
        
        queue.stats.completed++;
        this.stats.completed++;
        this.completed.add(jobId);
        
        // ✅ Persist to SQLite if ready
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('ack', {
                    queue: queueName,
                    jobId: jobId
                });
            } catch (error) {
                console.error('[QueueManager] ❌ Failed to persist ack:', error.message);
            }
        }
        
        this.emit('job.acked', { queueName, jobId });
        return true;
    }

    /**
     * ✅ Complete job with persistence
     */
    async complete(queueName, jobId, data) {
        const queue = this.getQueue(queueName);
        const inFlight = this.inFlight.get(jobId);
        
        // Remove from memory
        if (inFlight) {
            this.inFlight.delete(jobId);
            queue.processing.delete(jobId);
        }
        queue.completed.add(jobId);
        this.stats.completed++;
        this.completed.add(jobId);
        
        // ✅ Persist to SQLite if ready
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('complete', {
                    queue: queueName,
                    jobId: jobId,
                    data: data || {}
                });
            } catch (error) {
                console.error('[QueueManager] ❌ Failed to persist complete:', error.message);
            }
        }
        
        this.emit('job.completed', { queueName, jobId });
        return true;
    }

    /**
     * ✅ Requeue with persistence
     */
    async requeue(queueName, jobId) {
        const inFlight = this.inFlight.get(jobId);
        if (!inFlight) {
            this.emit('requeue.unknown', { queueName, jobId });
            return false;
        }
        
        const job = inFlight.job;
        job.attempts++;
        
        if (job.attempts >= job.maxAttempts) {
            // Dead letter
            this.deadLetter.push({
                job,
                queue: queueName,
                failedAt: Date.now(),
                attempts: job.attempts,
                error: job.error || 'Max retries exceeded'
            });
            this.stats.deadLettered++;
            this.inFlight.delete(jobId);
            const queue = this.getQueue(queueName);
            queue.processing.delete(jobId);
            
            // ✅ Persist to SQLite if ready
            if (this.persistenceReady) {
                try {
                    await this.sendSQLiteRequest('deadletter', {
                        queue: queueName,
                        jobId: jobId,
                        data: job
                    });
                } catch (error) {
                    console.error('[QueueManager] ❌ Failed to persist deadletter:', error.message);
                }
            }
            
            this.emit('job.deadletter', { queueName, jobId, job });
            return false;
        }
        
        // Requeue
        job.status = 'pending';
        job.dequeuedAt = null;
        job.workerId = null;
        job.expiresAt = null;
        this.inFlight.delete(jobId);
        const queue = this.getQueue(queueName);
        queue.processing.delete(jobId);
        queue.pending.unshift(job);
        this.stats.failed++;
        
        // ✅ Persist to SQLite if ready
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('requeue', {
                    queue: queueName,
                    jobId: jobId
                });
            } catch (error) {
                console.error('[QueueManager] ❌ Failed to persist requeue:', error.message);
            }
        }
        
        this.emit('job.requeued', { queueName, jobId, attempts: job.attempts });
        return true;
    }

    /**
     * ✅ Sweep timed out jobs with persistence
     */
    sweep() {
        const now = Date.now();
        let timedOut = 0;
        
        for (const [jobId, inFlight] of this.inFlight) {
            if (now > inFlight.expiresAt) {
                timedOut++;
                this.stats.timedOut++;
                this.emit('job.timeout', { queueName: inFlight.queue, jobId });
                this.requeue(inFlight.queue, jobId);
            }
        }
        
        if (timedOut > 0) {
            console.log(`[QueueManager] Sweep: ${timedOut} jobs timed out`);
        }
    }

    startSweeper() { 
        this.sweeper = setInterval(() => this.sweep(), this.options.sweepInterval); 
    }

    startFlusher() { 
        this.flushInterval = setInterval(() => {
            if (this.persistenceReady && this.pendingWrites.length > 0) {
                this.flush();
            }
        }, 1000); 
    }

    async flush() {
        if (this.pendingWrites.length === 0) return;
        const batch = this.pendingWrites.splice(0, this.options.batchSize);
        
        if (this.persistenceReady) {
            try {
                await this.sendSQLiteRequest('batch_write', { data: batch });
                this.emit('flush.complete', { count: batch.length });
            } catch (error) {
                console.error('[QueueManager] Flush error:', error.message);
                this.pendingWrites.unshift(...batch);
            }
        }
    }

    generateJobId() { 
        return `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`; 
    }

    getStats() {
        const queueNames = Array.from(this.queues.keys());
        const stats = { 
            queues: {}, 
            total: { ...this.stats }, 
            inFlight: this.inFlight.size, 
            deadLetter: this.deadLetter.length,
            persistenceReady: this.persistenceReady,
            pendingRequests: this.pendingRequests.size
        };
        for (const name of queueNames) {
            stats.queues[name] = this.getQueueStats(name);
        }
        return stats;
    }

    getQueueStats(queueName) {
        const queue = this.queues.get(queueName);
        if (!queue) return null;
        const inFlightCount = Array.from(this.inFlight.values())
            .filter(ifj => ifj.queue === queueName).length;
        return {
            name: queueName,
            pending: queue.pending.length,
            processing: queue.processing.size,
            inFlight: inFlightCount,
            completed: queue.completed.size,
            failed: queue.failed.size,
            deadLetter: this.deadLetter.filter(dl => dl.queue === queueName).length,
            stats: queue.stats,
        };
    }

    shutdown() {
        console.log('[QueueManager] Shutting down...');
        clearInterval(this.sweeper);
        clearInterval(this.flushInterval);
        
        // Final flush
        if (this.pendingWrites.length > 0) {
            this.flush();
        }
        
        this.queues.clear();
        this.inFlight.clear();
        this.deadLetter = [];
        this.completed = new Set();
        this.pendingWrites = [];
        this.pendingRequests.clear();
        this.removeAllListeners();
        console.log('[QueueManager] Shutdown complete');
    }
}

module.exports = QueueManager;