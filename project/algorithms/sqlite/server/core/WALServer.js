const { WebSocketServer } = require("ws");
const WALStore = require("./WALStore");
const WALLogger = require("./WALLogger");

class WALServer {
    constructor({
        port = 8080,
        dbPath
    } = {}) {
        this.port = port;

        this.store = new WALStore({ dbPath });
        this.logger = new WALLogger("walServer");

        this.wss = null;
    }

    handleMessage(ws, raw) {
        const msg = JSON.parse(raw);

        switch (msg.op) {
            case "recover": {
                const rows = this.store.select(msg.queue);

                ws.send(JSON.stringify({
                    op: "recover",
                    queue: msg.queue,
                    rows
                }));

                break;
            }

            case "ack": {
                const acked = this.store.ack(msg);

                if (!acked) {
                    this.logger.log("ack: no matching pending row found", {
                        queue: msg.queue,
                        id: msg.id
                    });
                }

                ws.send(JSON.stringify({
                    op: "ack",
                    id: msg.id,
                    queue: msg.queue,
                    acked
                }));

                return acked;
            }

            default:
                this.store.insert(msg);
                break;
        }
    }

    listen() {
        this.wss = new WebSocketServer({ port: this.port });

        this.wss.on("listening", () => {
            this.logger.log("running", { port: this.port });

            if (process.send) {
                process.send({ type: "ready" });
            }
        });

        this.wss.on("error", (err) => {
            this.logger.error("failed to start", err);
            process.exit(1);
        });

        this.wss.on("connection", ws => {
            ws.on("message", raw => this.handleMessage(ws, raw));
        });

        return this.wss;
    }
}

module.exports = WALServer;