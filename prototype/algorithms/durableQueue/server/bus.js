const WebSocket = require("ws");

class Bus {
    constructor(url = "ws://localhost:8080") {
        this.ws = new WebSocket(url);

        this.ready = new Promise(resolve => {
            this.ws.on("open", resolve);
        });
    }

    async send(msg) {
        await this.ready;
        this.ws.send(JSON.stringify(msg));
    }

    request(msg) {
        return new Promise(async (resolve, reject) => {
            await this.ready;

            const onMessage = raw => {
                this.ws.off("message", onMessage);

                try {
                    resolve(JSON.parse(raw));
                } catch (err) {
                    reject(err);
                }
            };

            this.ws.on("message", onMessage);

            this.ws.send(JSON.stringify(msg));
        });
    }
}

module.exports = Bus