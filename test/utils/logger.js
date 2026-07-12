// utils/logger.js
class Logger {
    constructor(options = {}) {
        this.level = options.level || 'info';
        this.prefix = options.prefix || 'APP';
        this.levels = {
            debug: 0,
            info: 1,
            warn: 2,
            error: 3
        };
    }

    _log(level, message, data = null) {
        const timestamp = new Date().toISOString();
        const prefix = `[${timestamp}] [${this.prefix}] [${level.toUpperCase()}]`;
        
        if (data) {
            console.log(`${prefix} ${message}`, data);
        } else {
            console.log(`${prefix} ${message}`);
        }
    }

    debug(message, data = null) {
        if (this.levels[this.level] <= this.levels.debug) {
            this._log('debug', message, data);
        }
    }

    info(message, data = null) {
        if (this.levels[this.level] <= this.levels.info) {
            this._log('info', message, data);
        }
    }

    warn(message, data = null) {
        if (this.levels[this.level] <= this.levels.warn) {
            this._log('warn', message, data);
        }
    }

    error(message, data = null) {
        if (this.levels[this.level] <= this.levels.error) {
            this._log('error', message, data);
        }
    }
}

module.exports = Logger;