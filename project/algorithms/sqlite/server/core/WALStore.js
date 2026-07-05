const path = require("path");
const { DatabaseSync } = require("node:sqlite");

class WALStore {
    constructor({
        dbPath = path.join(__dirname, "..","..","durableQueue", "wal.db")
    } = {}) {
        this.dbPath = dbPath;
        this.db = new DatabaseSync(this.dbPath);

        this._createSchema();
        this._prepareStatements();
    }

    _createSchema() {
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS events(
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                queue TEXT NOT NULL,
                op TEXT NOT NULL,
                id TEXT NOT NULL,
                payload TEXT,
                ts INTEGER NOT NULL
            )
        `);
    }

    _prepareStatements() {
        this.insertStmt = this.db.prepare(`
            INSERT INTO events(queue, op, id, payload, ts)
            VALUES (?, ?, ?, ?, ?)
        `);

        // Exclude already-acked rows so recover() only replays pending events
        this.selectStmt = this.db.prepare(`
            SELECT *
            FROM events
            WHERE queue = ? AND op != 'ack'
            ORDER BY seq
        `);

        // Mark all pending rows for this queue+id as acknowledged
        this.ackStmt = this.db.prepare(`
            UPDATE events
            SET op = 'ack'
            WHERE queue = ? AND id = ? AND op != 'ack'
        `);
    }

    insert(msg) {
        this.insertStmt.run(
            msg.queue,
            msg.op,
            msg.id,
            msg.payload ? JSON.stringify(msg.payload) : null,
            Date.now()
        );
    }

    select(queueName) {
        return this.selectStmt.all(queueName);
    }

    ack(msg) {
        const result = this.ackStmt.run(msg.queue, msg.id);
        // console.log(result);
        return result.changes > 0;
    }
}

module.exports = WALStore;