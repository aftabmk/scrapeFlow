// sqlite-server/components/response-sender.js
const { EventEmitter } = require('events');

class ResponseSender extends EventEmitter {
    constructor(options = {}) {
        super();
        this.sendFn = options.sendFn || null;
        this.isRunning = true;
    }

    /**
     * Set the send function
     */
    setSendFn(fn) {
        this.sendFn = fn;
    }

    /**
     * Send a response
     */
    send(jobId, data, targetPid) {
        if (!this.sendFn) {
            console.error('[ResponseSender] ❌ No send function set');
            return false;
        }

        try {
            this.sendFn(jobId, data, targetPid);
            this.emit('sent', { jobId, targetPid, data });
            return true;
        } catch (error) {
            console.error('[ResponseSender] ❌ Failed to send response:', error.message);
            this.emit('error', error);
            return false;
        }
    }

    /**
     * Send error response
     */
    sendError(jobId, error, targetPid) {
        return this.send(jobId, { error: error.message }, targetPid);
    }

    /**
     * Send success response
     */
    sendSuccess(jobId, data, targetPid) {
        return this.send(jobId, { success: true, ...data }, targetPid);
    }

    /**
     * Shutdown
     */
    shutdown() {
        this.isRunning = false;
        this.emit('shutdown');
    }
}

module.exports = ResponseSender;