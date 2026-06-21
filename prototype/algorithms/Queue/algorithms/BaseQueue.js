const WriteAheadLog = require("../models/WAL");

class BaseQueue {
	constructor({ visibilityTimeout = 30_000, maxMessages = 3, waitTimeMs = 5_000, queueId } = {}) {
		this.waitTimeMs = waitTimeMs;
		this.maxMessages = maxMessages;
		this.visibilityTimeout = visibilityTimeout;

		this.pending = [];
		this.store = new Map();
		this.inFlight = new Map();

		this.wal = new WriteAheadLog(queueId);

		// A constructor can't be async, so this can't block returning —
		// `new BaseQueue(...)` always returns immediately, before
		// _restore() has actually finished. `this.ready` is the promise
		// for that in-progress restore; every public method below awaits
		// it FIRST, so callers never have to remember `await q.ready`
		// themselves — it's enforced internally instead.
		this.ready = this._restore();
	}

	async _restore() {
		const entries = await this.wal.replay();

		for (const entry of entries) {
			if (entry.op === "enqueue") {
				this.store.set(entry.id, entry.event);
			} else if (entry.op === "ack") {
				this.store.delete(entry.id);
			}
		}

		this.pending = [...this.store.keys()];
	}

	// _compactWal() {
	// 	if (this.pending.length === 0 && this.inFlight.size === 0 && this.store.size === 0) {
	// 		this.wal.clear();    // implement this in WAL.js
	// 	}
	// }

	async enqueue(event) {
		await this.ready;

		this.wal.append({op: "enqueue",id: event.id,event});
		this.store.set(event.id, event);
		this.pending.push(event.id);
	}

	async ack(id) {
		await this.ready;

		this.wal.append({
			op: "ack",
			id,
		});
		const timer = this.inFlight.get(id);
		if (timer) {
			clearTimeout(timer);
			this.inFlight.delete(id);
		}
		this.store.delete(id);
		// this._compactWal();
	}

	async timeout(id) {
		await this.ready;

		if (!this.inFlight.has(id))
			return;

		this.inFlight.delete(id);

		if (this.store.has(id)) {
			this.pending.push(id);
		}
	}

	sleep(ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	}
}

module.exports = BaseQueue;