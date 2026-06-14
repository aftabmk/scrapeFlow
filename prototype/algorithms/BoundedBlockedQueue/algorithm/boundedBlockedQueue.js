class BoundedBlockingQueue {
    constructor(capacity) {
        this.capacity = capacity;
        this.queue = [];

        this.waitingConsumers = [];
        this.waitingProducers = [];
    }

    async enqueue(item) {
        // If a consumer is already waiting, deliver directly
        if (this.waitingConsumers.length > 0) {
            const consumerResolve = this.waitingConsumers.shift();
            consumerResolve(item);
            return;
        }

        // Queue has space
        if (this.queue.length < this.capacity) {
            this.queue.push(item);
            return;
        }

        // Queue full -> wait
        await new Promise(resolve => {
            this.waitingProducers.push({ item, resolve });
        });
    }

    async dequeue() {
        // Queue has items
        if (this.queue.length > 0) {
            const item = this.queue.shift();

            // If a producer is waiting, let one in
            if (this.waitingProducers.length > 0) {
                const { item: producerItem, resolve } =
                    this.waitingProducers.shift();

                this.queue.push(producerItem);
                resolve();
            }

            return item;
        }

        // Queue empty -> wait
        return new Promise(resolve => {
            this.waitingConsumers.push(resolve);
        });
    }

    size() {
        return this.queue.length;
    }
}

module.exports = BoundedBlockingQueue;