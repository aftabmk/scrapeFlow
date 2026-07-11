// sqlite-server/workers/write-worker.js
// ✅ Receives requests, calls sql-queries.js functions
const { EventEmitter } = require('events');

class SQLiteWriteWorker extends EventEmitter {
    constructor(options = {}) {
        super();
        this.workerId = options.workerId || 'write_worker_1';
        this.writeQueue = options.writeQueue;
        this.queries = options.queries;  // ✅ SQL queries instance
        this.sendResponse = options.sendResponse;
        this.isRunning = true;
        this.batchSize = 10;
        this.batchTimeout = 100;

        console.log(`[WriteWorker] ✅ Started: ${this.workerId}`);
        this._startProcessing();
    }

    async _startProcessing() {
        while (this.isRunning) {
            try {
                await this._processBatch();
            } catch (error) {
                console.error(`[WriteWorker] Error:`, error.message);
                await this._sleep(100);
            }
        }
    }

    async _processBatch() {
        const batch = [];
        let batchStart = Date.now();

        while (batch.length < this.batchSize && (Date.now() - batchStart) < this.batchTimeout) {
            const request = this.writeQueue.dequeue();
            if (request) {
                batch.push(request);
            } else {
                await this._sleep(10);
            }
        }

        if (batch.length === 0) {
            await this._sleep(50);
            return;
        }

        for (const request of batch) {
            if (!this.isRunning) break;
            await this._processRequest(request);
        }
    }

    async _processRequest(request) {
        const { op, queue, jobId, payload, sourcePid, requestId } = request;

        try {
            let result;

            // ✅ Map operation names to sql-queries.js functions
            const operationMap = {
                'append': () => this.queries.append(queue, jobId, payload),
                'deliver': () => this.queries.deliver(queue, jobId),
                'ack': () => this.queries.ack(queue, jobId),
                'requeue': () => this.queries.requeue(queue, jobId),
                'deadletter': () => this.queries.deadletter(queue, jobId, payload)
            };

            if (operationMap[op]) {
                result = operationMap[op]();
            } else {
                throw new Error(`Unknown write operation: ${op}`);
            }

            this.sendResponse(requestId, result, sourcePid);

        } catch (error) {
            console.error(`[WriteWorker] ${op} failed:`, error.message);
            this.sendResponse(requestId, { error: error.message }, sourcePid);
        }
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    shutdown() {
        this.isRunning = false;
        this.emit('shutdown', { workerId: this.workerId });
    }
}

module.exports = SQLiteWriteWorker;