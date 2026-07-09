const RecoverableQueue = require("./RecoverableQueue");

class DurableQueue extends RecoverableQueue {

    constructor(
        name,
        {
            visibilityTimeout = 30000,
            maxRetries = 5
        } = {}
    ) {

        super(name);

        this.visibilityTimeout = visibilityTimeout;
        this.maxRetries = maxRetries;

        this.inFlight = new Map();
        this.deadLetter = [];

        this.startSweeper();
    }

    startSweeper() {

        setInterval(() => {

            const now = Date.now();

            for (const [id, state] of this.inFlight) {

                if (now < state.expiresAt)
                    continue;

                this.inFlight.delete(id);

                state.retries++;

                if (state.retries > this.maxRetries) {
                    this.deadLetter.push(state.job);
                    continue;
                }

                this._pushBack(state.job);
            }

        }, 1000);
    }

    async dequeue() {

        const job = await super.dequeue();

        if (!job)
            return null;

        this.inFlight.set(job.id, {
            job,
            retries: 0,
            expiresAt: Date.now() + this.visibilityTimeout
        });

        return job;
    }

    async dequeueBatch(batchSize = 1) {
        const jobs = [];

        for (let i = 0; i < batchSize; i++) {
            const job = await this.dequeue();
            if (!job) break; // queue drained, stop early
            jobs.push(job);
        }

        return jobs;
    }

    async ack(id) {

        if (!this.inFlight.has(id))
            return false;

        await super.ack(id);

        this.inFlight.delete(id);

        return true;
    }
}

module.exports = DurableQueue;