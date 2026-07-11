// workers/sqlite-comm-worker.js
const { EventEmitter } = require('events');

class SQLiteCommWorker extends EventEmitter {
    constructor(options = {}) {
        super();

        this.workerId = options.workerId || `sqlite_comm_${process.pid}`;
        this.writeQueue = options.writeQueue;
        this.queueName = options.queueName || 'default_queue';
        this.pollInterval = options.pollInterval || 100;
        this.batchSize = options.batchSize || 50;
        this.isRunning = true;

        this.pendingRequests = new Map();
        this.stats = {
            batchesProcessed: 0,
            operationsProcessed: 0,
            failedOperations: 0
        };

        this._setupIPCListener();
        this._startProcessing();
    }

    // === IPC Setup ===

    _setupIPCListener() {
        process.on('message', (message) => {
            // ✅ Response contains jobId (exchange-contract)
            if (message && message.jobId) {
                this._handleResponse(message);
            }
        });
    }

    _handleResponse(response) {
        const { jobId, error, ...data } = response;

        // ✅ jobId is exchange-contract
        if (this.pendingRequests.has(jobId)) {
            const { resolve, reject, timeout } = this.pendingRequests.get(jobId);
            clearTimeout(timeout);
            this.pendingRequests.delete(jobId);

            if (error) {
                reject(new Error(error));
            } else {
                resolve(data);
            }
        }
    }

    // === Public API ===

    async sendRequest(op, data = {}) {
        return this._sendRequest(op, data);
    }

    // === Private Request ===

    // workers/sqlite-comm-worker.js
    _sendRequest(op, data = {}) {
        return new Promise((resolve, reject) => {
            // ✅ Use queue name as tracking ID for recover
            let trackId;
            if (op === 'recover') {
                // when app is restarted retrive all un-acked data from queue
                // Use queue name - this is consistent and predictable
                trackId = `recover_${this.queueName}`;
            } else {
                // ✅ For job operations, use jobId (exchange-contract)
                trackId = data.jobId;
            }

            if (!trackId) {
                reject(new Error(`Missing tracking ID for operation: ${op}`));
                return;
            }

            const timeout = setTimeout(() => {
                this.pendingRequests.delete(trackId);
                reject(new Error(`Request timeout: ${op} (${trackId})`));
                // 100 ms works for local, production 500 will be preferable
            }, 100);

            this.pendingRequests.set(trackId, { resolve, reject, timeout });

            // ✅ Build message
            const message = {
                type: 'SQLITE_REQUEST',
                data: {
                    op,
                    queue: this.queueName,
                    sourcePid: process.pid,
                    ...data
                }
            };

            // ✅ For recover: use queue name as jobId for tracking
            if (op === 'recover') {
                message.data.jobId = trackId;  // ✅ recover_browser_queue (consistent)
            } else {
                message.data.jobId = trackId;  // ✅ exchange-contract
            }

            if (data.payload) {
                message.data.payload = data.payload;
            }
            if (data.retries !== undefined) {
                message.data.retries = data.retries;
            }

            console.log(`[SQLiteCommWorker] 📨 Sending ${op} with jobId: ${trackId}`);
            process.send(message);
        });
    }

    // === Processing Loop ===

    async _startProcessing() {
        while (this.isRunning) {
            try {
                await this._processBatch();
            } catch (error) {
                console.error(`[SQLiteCommWorker] Error:`, error.message);
                await this._sleep(100);
            }
        }
    }

    async _processBatch() {
        if (this.writeQueue.isEmpty()) {
            await this._sleep(this.pollInterval);
            return;
        }

        const batch = this.writeQueue.dequeueBatch(this.batchSize);

        if (batch.length === 0) {
            await this._sleep(this.pollInterval);
            return;
        }

        try {
            await this._processBatchOperations(batch);
            this.stats.batchesProcessed++;
            this.stats.operationsProcessed += batch.length;
        } catch (error) {
            console.error(`[SQLiteCommWorker] Batch failed:`, error.message);
            this.stats.failedOperations += batch.length;

            for (const op of batch) {
                this.writeQueue.enqueue(op);
            }
        }
    }

    async _processBatchOperations(batch) {
        const groups = {
            append: [],
            deliver: [],
            ack: [],
            requeue: [],
            deadletter: []
        };

        for (const op of batch) {
            if (groups[op.type]) {
                groups[op.type].push(op);
            }
        }

        for (const [type, ops] of Object.entries(groups)) {
            if (ops.length === 0) continue;

            try {
                if (type === 'append') {
                    await this._processAppends(ops);
                } else if (type === 'deliver') {
                    await this._processDelivers(ops);
                } else if (type === 'ack') {
                    await this._processAcks(ops);
                } else if (type === 'requeue') {
                    await this._processRequeues(ops);
                } else if (type === 'deadletter') {
                    await this._processDeadLetters(ops);
                }
            } catch (error) {
                console.error(`[SQLiteCommWorker] ${type} failed:`, error.message);
                for (const op of ops) {
                    this.writeQueue.enqueue(op);
                }
                throw error;
            }
        }
    }

    // === Operation Processors ===

    async _processAppends(ops) {
        for (const op of ops) {
            // ✅ op.jobId is exchange-contract
            await this._sendRequest('append', {
                jobId: op.jobId,
                payload: op.payload
            });
        }
    }

    async _processDelivers(ops) {
        for (const op of ops) {
            await this._sendRequest('deliver', {
                jobId: op.jobId
            });
        }
    }

    async _processAcks(ops) {
        for (const op of ops) {
            await this._sendRequest('ack', {
                jobId: op.jobId
            });
        }
    }

    async _processRequeues(ops) {
        for (const op of ops) {
            await this._sendRequest('requeue', {
                jobId: op.jobId
            });
        }
    }

    async _processDeadLetters(ops) {
        for (const op of ops) {
            await this._sendRequest('deadletter', {
                jobId: op.jobId,
                payload: op.payload,
                retries: op.retries || 0
            });
        }
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    shutdown() {
        this.isRunning = false;

        for (const [jobId, { reject }] of this.pendingRequests) {
            reject(new Error('SQLiteCommWorker shutting down'));
            this.pendingRequests.delete(jobId);
        }

        this.emit('shutdown', { workerId: this.workerId });
    }

    getStats() {
        return {
            workerId: this.workerId,
            ...this.stats,
            writeQueueSize: this.writeQueue.size(),
            pendingRequests: this.pendingRequests.size
        };
    }
}

module.exports = SQLiteCommWorker;