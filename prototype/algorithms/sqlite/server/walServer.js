const path = require("path");

const { WebSocketServer } = require("ws");
const { DatabaseSync } = require("node:sqlite");

class WALServer {
    constructor({
        port = 8080,
        dbPath = path.join(__dirname, "..", "durableQueue", "wal.db")
    } = {}) {
        this.port = port;
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

        this.wss = null;
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

    handleMessage(ws, raw) {
        const msg = JSON.parse(raw);

        switch (msg.op) {
            case "recover": {
                const rows = this.select(msg.queue);

                ws.send(JSON.stringify({
                    op: "recover",
                    queue: msg.queue,
                    rows
                }));

                break;
            }

            default:
                this.insert(msg);
                break;
        }
    }

    listen() {
        this.wss = new WebSocketServer({ port: this.port });

        this.wss.on("connection", ws => {
            ws.on("message", raw => this.handleMessage(ws, raw));
        });

        console.log("WAL server running");

        return this.wss;
    }
}

module.exports = WALServer;

if (require.main === module) {
    new WALServer().listen();
}