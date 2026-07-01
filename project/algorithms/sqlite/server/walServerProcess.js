const path = require("path");
const { DatabaseSync } = require("node:sqlite");

class WALServer {
    constructor({
        dbPath = path.join(__dirname, "..", "durableQueue", "wal.db")
    } = {}) {
        this.dbPath = dbPath;

        this.db = new DatabaseSync(this.dbPath);

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

        this.insertStmt = this.db.prepare(`
            INSERT INTO events(queue, op, id, payload, ts)
            VALUES (?, ?, ?, ?, ?)
        `);

        this.selectStmt = this.db.prepare(`
            SELECT *
            FROM events
            WHERE queue = ?
            ORDER BY seq
        `);
    }

    log(message, extra = {}) {
        if (process.send) {
            process.send({ type: "log", source: "walServer", message, ...extra });
        } else {
            console.log(`[walServer] ${message}`);
        }
    }

    error(message, err) {
        if (process.send) {
            process.send({
                type: "error",
                source: "walServer",
                message,
                error: err ? err.message : undefined
            });
        } else {
            console.error(`[walServer] ${message}`, err);
        }
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

    handleMessage(msg) {
        switch (msg.op) {
            case "recover": {
                const rows = this.select(msg.queue);

                process.send({
                    type: "recover-response",
                    requestId: msg.requestId,
                    queue: msg.queue,
                    rows
                });

                break;
            }

            default:
                this.insert(msg);

                // ack the write back to whoever sent it, if they asked for one
                if (msg.requestId) {
                    process.send({
                        type: "write-ack",
                        requestId: msg.requestId,
                        id: msg.id
                    });
                }

                break;
        }
    }

    listen() {
        process.on("message", (msg) => {
            try {
                this.handleMessage(msg);
            } catch (err) {
                this.error("failed to handle message", err);
            }
        });

        this.log("running");

        if (process.send) {
            process.send({ type: "ready" });
        }
    }
}

module.exports = WALServer;

if (require.main === module) {
    new WALServer().listen();
}