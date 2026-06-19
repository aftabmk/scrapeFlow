const WriteAheadLog = require("../models/WAL");

class BaseQueue {
	constructor({ visibilityTimeout = 30_000, maxMessages = 3, waitTimeMs = 5_000, walPath = "./queue.wal" } = {}) {
		this.waitTimeMs = waitTimeMs;
		this.maxMessages = maxMessages;
		this.visibilityTimeout = visibilityTimeout;

		this.pending = [];
		this.store = new Map();
		this.inFlight = new Map();

		this.wal = new WriteAheadLog(walPath);

		this._restore();
	}

	_restore() {
		const entries = this.wal.replay();

		for (const entry of entries) {
			if (entry.op === "enqueue") {
				this.store.set(entry.id, entry.event);
			} else if (entry.op === "ack") {
				this.store.delete(entry.id);
			}
		}

		this.pending = [...this.store.keys()];
	}

	_compactWal() {
		if (this.pending.length === 0 && this.inFlight.size === 0 && this.store.size === 0) {
			this.wal.clear();    // implement this in WAL.js
		}
	}

	enqueue(event) {
		this.wal.append({op: "enqueue",id: event.id,event});
		this.store.set(event.id, event);
		this.pending.push(event.id);
	}

	ack(id) {
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

	timeout(id) {
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