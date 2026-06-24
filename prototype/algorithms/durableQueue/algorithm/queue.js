const Node = require("./node");

class Queue {
    constructor(
        name,
        bus,
        {
            visibilityTimeout = 30000,
            maxRetries = 5
        } = {}
    ) {
        this.name = name;
        this.bus = bus;

        this.head = null;
        this.tail = null;

        this.pending = new Map();
        this.inFlight = new Map();
        this.deadLetter = [];

        this.visibilityTimeout = visibilityTimeout;
        this.maxRetries = maxRetries;

        this.sweeper = this.startSweeper();
    }

    startSweeper() {
        return setInterval(() => {
            const now = Date.now();

            for (const [id, state] of this.inFlight) {

                if (now < state.expiresAt)
                    continue;

                this.inFlight.delete(id);

                state.retries++;

                if (state.retries > this.maxRetries) {
                    console.log("dead letter", id);
                    this.deadLetter.push(state.job);
                    continue;
                }

                this._pushBack(state.job, state.retries);
            }

        }, 1000);
    }

    stopSweeper() {
        clearInterval(this.sweeper);
    }

    _pushBack(job) {
        const node = new Node(job);

        if (!this.head) {
            this.head = this.tail = node;
        } else {
            this.tail.next = node;
            node.prev = this.tail;
            this.tail = node;
        }

        this.pending.set(job.id, node);
    }

    async enqueue(job) {
        this._pushBack(job);

        return this.bus.send({
            op: "append",
            queue: this.name,
            id: job.id,
            payload: job
        });
    }

    async dequeue() {
        if (!this.head)
            return null;

        const node = this.head;

        this.head = node.next;

        if (this.head)
            this.head.prev = null;
        else
            this.tail = null;

        this.pending.delete(node.job.id);

        this.inFlight.set(node.job.id, {
            job: node.job,
            retries: 0,
            expiresAt: Date.now() + this.visibilityTimeout
        });

        await this.bus.send({
            op: "deliver",
            queue: this.name,
            id: node.job.id
        });

        return node.job;
    }

    async ack(id) {
        const state = this.inFlight.get(id);

        if (!state)
            return false;

        await this.bus.send({
            op: "ack",
            queue: this.name,
            id
        });

        this.inFlight.delete(id);

        return true;
    }

    size() {
        return this.pending.size;
    }

    empty() {
        return this.pending.size === 0;
    }
}

module.exports = Queue;