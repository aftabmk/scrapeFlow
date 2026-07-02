const Queue = require("./Queue");

class RecoverableQueue extends Queue {

    async recover() {

        const { rows } = await this.bus.request({
            op: "recover",
            queue: this.name
        });

        const active = new Map();

        for (const row of rows) {
            switch (row.op) {
                case "append":
                    active.set(row.id,JSON.parse(row.payload));
                    break;
                case "ack":
                    active.delete(row.id);
                    break;
            }
        }

        this.pending.clear();
        this.head = this.tail = null;

        for (const job of active.values())
            this._pushBack(job);
    }
}

module.exports = RecoverableQueue;