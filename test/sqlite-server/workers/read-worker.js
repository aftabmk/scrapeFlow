// sqlite-server/workers/read-worker.js
// ✅ Receives requests, calls sql-queries.js functions
const { EventEmitter } = require('events');

class SQLiteReadWorker extends EventEmitter {
    constructor(options = {}) {
        super();
        this.workerId = options.workerId || 'read_worker_1';
        this.readQueue = options.readQueue;
        this.queries = options.queries;  // ✅ SQL queries instance
        this.sendResponse = options.sendResponse;
        this.isRunning = true;

        console.log(`[ReadWorker] ✅ Started: ${this.workerId}`);
        this._startProcessing();
    }

    async _startProcessing() {
        while (this.isRunning) {
            try {
                await this._processRequest();
            } catch (error) {
                console.error(`[ReadWorker ${this.workerId}] Error:`, error.message);
                await this._sleep(100);
            }
        }
    }

    async _processRequest() {
        const request = this.readQueue.dequeue();

        if (!request) {
            await this._sleep(10);
            return;
        }

        const { op, queue, jobId, payload, sourcePid, requestId } = request;

        try {
            let result;

            // ✅ Map operation names to sql-queries.js functions
            const operationMap = {
                'dequeue': () => this.queries.dequeue(queue, jobId),
                'dequeue_multiple': () => this.queries.dequeueMultiple(queue, jobId, payload?.count || 1),
                'recover': () => this.queries.recover(queue),
                'stats': () => this.queries.stats(queue)
            };

            if (operationMap[op]) {
                result = operationMap[op]();
            } else {
                throw new Error(`Unknown read operation: ${op}`);
            }

            this.sendResponse(requestId, result, sourcePid);

        } catch (error) {
            console.error(`[ReadWorker ${this.workerId}] ${op} failed:`, error.message);
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

module.exports = SQLiteReadWorker;