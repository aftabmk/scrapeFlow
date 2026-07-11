// sqlite-server/components/request-router.js
const { EventEmitter } = require('events');
const Queue = require('../../queue/queue');

class RequestRouter extends EventEmitter {
    constructor(options = {}) {
        super();
        this.writeQueue = new Queue({ name: 'write_queue', maxSize: 10000 });
        this.readQueue = new Queue({ name: 'read_queue', maxSize: 10000 });
        this.isRunning = true;
    }

    /**
     * Route a request to the appropriate queue
     */
    route(request) {
        const { op } = request;

        const isWrite = ['append', 'deliver', 'ack', 'requeue', 'deadletter'].includes(op);
        const isRead = ['dequeue', 'dequeue_multiple', 'recover', 'stats'].includes(op);

        if (isWrite) {
            this.writeQueue.enqueue(request);
            this.emit('writeQueued', request);
            return { queued: true, queue: 'write' };
        } else if (isRead) {
            this.readQueue.enqueue(request);
            this.emit('readQueued', request);
            return { queued: true, queue: 'read' };
        } else {
            throw new Error(`Unknown operation: ${op}`);
        }
    }

    /**
     * Get write queue
     */
    getWriteQueue() {
        return this.writeQueue;
    }

    /**
     * Get read queue
     */
    getReadQueue() {
        return this.readQueue;
    }

    /**
     * Get queue sizes
     */
    getQueueSizes() {
        return {
            writeQueue: this.writeQueue.size(),
            readQueue: this.readQueue.size()
        };
    }

    /**
     * Clear all queues
     */
    clear() {
        this.writeQueue.clear();
        this.readQueue.clear();
    }

    /**
     * Shutdown
     */
    shutdown() {
        this.isRunning = false;
        this.emit('shutdown');
    }
}

module.exports = RequestRouter;