// sqlite-server/components/ipc-handler.js
const { EventEmitter } = require('events');

class IPCHandler extends EventEmitter {
    constructor(options = {}) {
        super();
        this.isRunning = true;
        this.requestRouter = options.requestRouter;
        this.responseSender = options.responseSender;
        this.sqliteManager = options.sqliteManager;
        this.requestsProcessed = 0;
    }

    /**
     * Start listening for IPC messages
     */
    start() {
        console.log('[IPCHandler] Starting IPC listener...');

        process.on('message', async (message) => {
            if (!message) return;

            try {
                await this._handleRequest(message);
                this.requestsProcessed++;
            } catch (error) {
                console.error('[IPCHandler] Error handling request:', error.message);
                if (this.responseSender) {
                    this.responseSender.sendError(message.jobId, error, message.sourcePid);
                }
            }
        });

        console.log('[IPCHandler] ✅ IPC listener ready');
        this.emit('started');
    }

    /**
     * Handle incoming request
     */
    async _handleRequest(request) {
        const { op, queue, jobId, payload, sourcePid } = request;

        // Ensure queue table exists
        if (this.sqliteManager) {
            this.sqliteManager.getQueueTable(queue);
        }

        // Route the request
        if (this.requestRouter) {
            const result = this.requestRouter.route(request);
            this.emit('requestRouted', { request, result });
            return result;
        }

        throw new Error('Request router not available');
    }

    /**
     * Get stats
     */
    getStats() {
        return {
            requestsProcessed: this.requestsProcessed,
            isRunning: this.isRunning
        };
    }

    /**
     * Shutdown
     */
    shutdown() {
        this.isRunning = false;
        this.emit('shutdown');
    }
}

module.exports = IPCHandler;