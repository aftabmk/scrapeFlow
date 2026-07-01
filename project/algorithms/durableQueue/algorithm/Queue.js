const LinkedQueue = require("./LinkedQueue");

class Queue extends LinkedQueue {

    constructor(name, bus) {
        super(name);

        // this.bus = bus;
    }

    async enqueue(job) {
        this._pushBack(job);

        await this.bus.send({
            op: "append",
            queue: this.name,
            id: job.id,
            payload: job
        });
    }

    async dequeue() {

        const job = this.popFront();

        if (!job)
            return null;

        await this.bus.send({
            op: "deliver",
            queue: this.name,
            id: job.id
        });

        return job;
    }

    async ack(id) {
        await this.bus.send({
            op: "ack",
            queue: this.name,
            id
        });
    }
}

module.exports = Queue;