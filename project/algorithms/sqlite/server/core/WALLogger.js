class WALLogger {
    constructor(source = "walServer") {
        this.source = source;
    }

    log(message, extra = {}) {
        if (process.send) {
            process.send({ type: "log", source: this.source, message, ...extra });
        } else {
            console.log(`[${this.source}] ${message}`);
        }
    }

    error(message, err) {
        if (process.send) {
            process.send({
                type: "error",
                source: this.source,
                message,
                error: err ? err.message : undefined
            });
        } else {
            console.error(`[${this.source}] ${message}`, err);
        }
    }
}

module.exports = WALLogger;