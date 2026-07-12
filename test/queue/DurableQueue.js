// queue/DurableQueue.js
const LinkedQueue = require('./LinkedQueue');
const WriteQueue = require('./WriteQueue');

class DurableQueue {
    constructor(name, options = {}) {
        this.name = name;
        this.visibilityTimeout = options.visibilityTimeout || 30000;
        this.maxRetries = options.maxRetries || 5;
        this.sweepInterval = options.sweepInterval || 1000;
        
        // ✅ serverReady flag - controls whether to recover
        this.serverReady = options.serverReady || false;
        this.recovered = false;
        
        this.queue = new LinkedQueue(name);
        this.inFlight = new Map();
        this.deadLetter = [];
        this.writeQueue = options.writeQueue || new WriteQueue();
        this.commWorker = options.commWorker;

        this.stats = {
            enqueued: 0,
            dequeued: 0,
            acked: 0,
            timedOut: 0,
            deadLettered: 0
        };

        this.isRunning = true;
        this.sweeperTimer = null;
        this._recoverPromise = null;

        // ✅ Only recover if serverReady is true (crash restart)
        if (this.serverReady) {
            console.log(`[DurableQueue:${this.name}] 🔄 Server ready - recovering...`);
            this._recoverPromise = this.recover().catch(err => {
                console.error(`[DurableQueue:${this.name}] Recovery failed:`, err);
            });
        } else {
            console.log(`[DurableQueue:${this.name}] ⏭️ Server not ready - skipping recover`);
            this.recovered = true;
        }

        this._startSweeper();
    }

    // === Sweeper for timed-out jobs ===

    _startSweeper() {
        this.sweeperTimer = setInterval(() => {
            if (!this.isRunning) return;

            const now = Date.now();
            let timedOut = 0;

            for (const [id, state] of this.inFlight) {
                if (now < state.expiresAt) continue;

                this.inFlight.delete(id);
                timedOut++;
                this.stats.timedOut++;

                state.retries++;

                if (state.retries > this.maxRetries) {
                    this.deadLetter.push({
                        job: state.job,
                        retries: state.retries,
                        failedAt: new Date().toISOString()
                    });
                    this.stats.deadLettered++;

                    this.writeQueue.enqueue({
                        type: 'deadletter',
                        jobId: id,
                        payload: state.job,
                        retries: state.retries
                    });

                    continue;
                }

                this.queue._pushBack(state.job);

                this.writeQueue.enqueue({
                    type: 'requeue',
                    jobId: id
                });
            }

            if (timedOut > 0) {
                console.log(`[DurableQueue:${this.name}] ⏰ ${timedOut} jobs timed out`);
            }

        }, this.sweepInterval);
    }

    // === Public API ===

    async enqueue(job) {
        if (this._recoverPromise) {
            await this._recoverPromise;
            this._recoverPromise = null;
            this.recovered = true;
        }

        if (!job.id) {
            throw new Error('Job must have an id property');
        }

        this.queue._pushBack(job);
        this.stats.enqueued++;

        this.writeQueue.enqueue({
            type: 'append',
            jobId: job.id,
            payload: job
        });

        return job.id;
    }

    async dequeue() {
        if (this._recoverPromise) {
            await this._recoverPromise;
            this._recoverPromise = null;
            this.recovered = true;
        }

        const job = this.queue.popFront();

        if (job) {
            this.inFlight.set(job.id, {
                job,
                retries: 0,
                expiresAt: Date.now() + this.visibilityTimeout,
                dequeuedAt: new Date().toISOString()
            });
            this.stats.dequeued++;

            this.writeQueue.enqueue({
                type: 'deliver',
                jobId: job.id
            });

            return job;
        }

        return null;
    }

    async dequeueBatch(batchSize = 1) {
        const jobs = [];
        const maxBatch = Math.min(batchSize, 10);

        for (let i = 0; i < maxBatch; i++) {
            const job = await this.dequeue();
            if (!job) break;
            jobs.push(job);
        }

        return jobs;
    }

    async ack(id) {
        if (!this.inFlight.has(id)) {
            console.warn(`[DurableQueue:${this.name}] ⚠️ ACK for unknown job: ${id}`);
            return false;
        }

        this.inFlight.delete(id);
        this.stats.acked++;

        this.writeQueue.enqueue({
            type: 'ack',
            jobId: id
        });

        return true;
    }

    async requeue(id) {
        if (!this.inFlight.has(id)) {
            console.warn(`[DurableQueue:${this.name}] ⚠️ Requeue for unknown job: ${id}`);
            return false;
        }

        const state = this.inFlight.get(id);
        this.inFlight.delete(id);

        state.retries++;
        this.queue._pushBack(state.job);

        this.writeQueue.enqueue({
            type: 'requeue',
            jobId: id
        });

        return true;
    }

    // === Recovery ===

    async recover() {
        try {
            if (this.commWorker && typeof this.commWorker.sendRequest === 'function') {
                const result = await this.commWorker.sendRequest('recover');
                
                if (result && result.rows) {
                    this.queue.clear();
                    this.inFlight.clear();

                    for (const row of result.rows) {
                        const job = typeof row.payload === 'string'
                            ? JSON.parse(row.payload)
                            : row.payload;

                        if (row.op === 'append' && job.id) {
                            this.queue._pushBack(job);
                        }
                    }

                    console.log(`[DurableQueue:${this.name}] 🔄 Recovered ${this.queue.getSize()} jobs`);
                }

                this.recovered = true;
                return this.queue.getSize();
            }

            console.warn(`[DurableQueue:${this.name}] ⚠️ No commWorker available`);
            this.recovered = true;
            return 0;
        } catch (error) {
            console.error(`[DurableQueue:${this.name}] Recovery failed:`, error.message);
            this.recovered = true;
            return 0;
        }
    }

    // === Stats ===

    getStats() {
        return {
            name: this.name,
            pending: this.queue.getSize(),
            inFlight: this.inFlight.size,
            deadLetter: this.deadLetter.length,
            writeQueueSize: this.writeQueue.size(),
            visibilityTimeout: this.visibilityTimeout,
            maxRetries: this.maxRetries,
            stats: this.stats,
            recovered: this.recovered
        };
    }

    getInFlightCount() {
        return this.inFlight.size;
    }

    getDeadLetterCount() {
        return this.deadLetter.length;
    }

    getWriteQueueSize() {
        return this.writeQueue.size();
    }

    clearDeadLetter() {
        const count = this.deadLetter.length;
        this.deadLetter = [];
        return count;
    }

    shutdown() {
        this.isRunning = false;
        if (this.sweeperTimer) {
            clearInterval(this.sweeperTimer);
            this.sweeperTimer = null;
        }

        const remaining = this.writeQueue.size();
        if (remaining > 0) {
            console.log(`[DurableQueue:${this.name}] ⏳ ${remaining} pending writes remaining`);
        }

        console.log(`[DurableQueue:${this.name}] 🛑 Shutdown complete`);
    }
}

module.exports = DurableQueue;