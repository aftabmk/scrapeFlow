const Queue = require("./Queue");
const boundedBlockedQueue = require('../../BoundedBlockedQueue/algorithm/boundedBlockedQueue');

class EventQueue extends Queue {
    constructor(eventSource, options = {}) {
        super(options);

        this.buffer = new boundedBlockedQueue(options.bufferSize ?? 1);

        eventSource.subscribe(event => {
            this.buffer.enqueue(event);
        });

        this.run();
    }

    async run() {
        for (;;) {
            const event = await this.buffer.dequeue();
            super.enqueue(event);
        }
    }
}

module.exports = EventQueue;