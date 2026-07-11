// queue/WriteQueue.js
class WriteQueue {
    constructor(options = {}) {
        this.queue = [];
        this.maxSize = options.maxSize || 10000;
        this.processing = false;
        this.stats = {
            enqueued: 0,
            dequeued: 0,
            dropped: 0
        };
    }

    enqueue(operation) {
        if (this.queue.length >= this.maxSize) {
            this.stats.dropped++;
            return false;
        }
        this.queue.push(operation);
        this.stats.enqueued++;
        return true;
    }

    dequeue() {
        const item = this.queue.shift();
        if (item) {
            this.stats.dequeued++;
        }
        return item || null;
    }

    dequeueBatch(batchSize = 50) {
        const batch = [];
        const size = Math.min(batchSize, this.queue.length);
        for (let i = 0; i < size; i++) {
            batch.push(this.queue.shift());
        }
        this.stats.dequeued += batch.length;
        return batch;
    }

    peek() {
        return this.queue[0] || null;
    }

    size() {
        return this.queue.length;
    }

    isEmpty() {
        return this.queue.length === 0;
    }

    clear() {
        this.queue = [];
    }

    getStats() {
        return {
            ...this.stats,
            currentSize: this.queue.length,
            maxSize: this.maxSize
        };
    }
}

module.exports = WriteQueue;