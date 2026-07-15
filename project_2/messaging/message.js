// messaging/message.js
const { MessageTypes, MessageDestinations, VALID_DESTINATIONS, VALID_TYPES } = require('./message-types');
const { performance } = require('perf_hooks');

/**
 * Message class for standardized IPC communication
 */
class Message {
    constructor(options = {}) {
        const {
            from,
            to,
            type,
            payload = {},
            requestId = null,
            replyTo = null,
            correlationId = null,
            timestamp = null,
            priority = 0,
            ttl = null,
            retryCount = 0,
            maxRetries = 3
        } = options;

        // === Required Fields ===
        if (!from) throw new Error('Message: "from" is required');
        if (!to) throw new Error('Message: "to" is required');
        if (!type) throw new Error('Message: "type" is required');
        if (!VALID_DESTINATIONS.includes(from) && from !== 'unknown') {
            throw new Error(`Message: invalid "from": ${from}`);
        }
        if (!VALID_DESTINATIONS.includes(to) && to !== 'unknown') {
            throw new Error(`Message: invalid "to": ${to}`);
        }
        if (!VALID_TYPES.includes(type)) {
            throw new Error(`Message: invalid "type": ${type}`);
        }

        // === Core Properties ===
        this.from = from;
        this.to = to;
        this.type = type;
        this.payload = payload;
        this.requestId = requestId || this._generateRequestId();
        this.replyTo = replyTo;
        this.correlationId = correlationId || this.requestId;
        this.timestamp = timestamp || Date.now();
        this.priority = priority;
        this.ttl = ttl;
        this.retryCount = retryCount;
        this.maxRetries = maxRetries;
        this._responsePromise = null;
        this._responseResolve = null;
        this._responseReject = null;
        this._pendingRequests = new Map();

        this._validate();
    }

    _generateRequestId() {
        return `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    }

    _validate() {
        if (this.ttl && this.ttl < 0) {
            throw new Error('Message: TTL must be positive');
        }
        if (this.priority < 0 || this.priority > 10) {
            throw new Error('Message: priority must be between 0 and 10');
        }
        if (this.retryCount > this.maxRetries) {
            throw new Error('Message: retry count exceeds max retries');
        }
    }

    isExpired() {
        if (!this.ttl) return false;
        return (Date.now() - this.timestamp) > this.ttl;
    }

    reply(responsePayload) {
        return new Message({
            from: this.to,
            to: this.from,
            type: `${this.type}_RESPONSE`,
            payload: responsePayload,
            requestId: this.requestId,
            correlationId: this.correlationId,
            replyTo: this.type
        });
    }

    send(target = null) {
        if (this.isExpired()) {
            console.warn(`[Message] ⚠️ Expired message: ${this.requestId}`);
            return false;
        }

        const messageData = this.toJSON();
        this._log('📤', messageData);

        if (target && typeof target.send === 'function') {
            target.send(messageData);
        } else if (process.send) {
            process.send(messageData);
        } else {
            console.error('[Message] ❌ No send target available');
            return false;
        }

        return true;
    }

    sendAndWait(timeout = 10000) {
        return new Promise((resolve, reject) => {
            if (this.isExpired()) {
                reject(new Error('Message expired before sending'));
                return;
            }

            this._responsePromise = { resolve, reject, timeout };
            this.send();

            const timeoutId = setTimeout(() => {
                if (this._responsePromise) {
                    this._responsePromise.reject(
                        new Error(`Response timeout for ${this.type} (${this.requestId})`)
                    );
                    this._responsePromise = null;
                }
            }, timeout);

            this._timeoutId = timeoutId;
        });
    }

    handleResponse(response) {
        if (this._responsePromise) {
            clearTimeout(this._timeoutId);
            this._responsePromise.resolve(response.payload);
            this._responsePromise = null;
        }
    }

    static response(message, payload) {
        return message.reply(payload);
    }

    static from(raw) {
        if (raw instanceof Message) return raw;
        return new Message({
            from: raw.from || 'unknown',
            to: raw.to || 'unknown',
            type: raw.type || 'UNKNOWN',
            payload: raw.payload || {},
            requestId: raw.requestId,
            replyTo: raw.replyTo,
            correlationId: raw.correlationId,
            timestamp: raw.timestamp,
            priority: raw.priority,
            ttl: raw.ttl,
            retryCount: raw.retryCount,
            maxRetries: raw.maxRetries
        });
    }

    toJSON() {
        return {
            from: this.from,
            to: this.to,
            type: this.type,
            payload: this.payload,
            requestId: this.requestId,
            replyTo: this.replyTo,
            correlationId: this.correlationId,
            timestamp: this.timestamp,
            priority: this.priority,
            ttl: this.ttl,
            retryCount: this.retryCount,
            maxRetries: this.maxRetries
        };
    }

    _log(direction, data) {
        const payloadStr = JSON.stringify(data.payload || {}).slice(0, 200);
        console.log(
            `${direction} [${data.from} → ${data.to}] ${data.type} ` +
            `(${data.requestId}) ${payloadStr}`
        );
    }

    static setupGlobalHandler() {
        if (!global._messageHandlerSetup) {
            global._messageHandlerSetup = true;
            global._pendingMessages = new Map();

            process.on('message', (raw) => {
                if (!raw || !raw.type) return;

                if (raw.requestId && global._pendingMessages.has(raw.requestId)) {
                    const pending = global._pendingMessages.get(raw.requestId);
                    clearTimeout(pending.timeoutId);
                    global._pendingMessages.delete(raw.requestId);
                    pending.resolve(raw.payload);
                    return;
                }

                process.emit('message_received', raw);
            });
        }
    }

    static registerPending(requestId, resolve, reject, timeoutId) {
        Message.setupGlobalHandler();
        global._pendingMessages.set(requestId, { resolve, reject, timeoutId });
    }

    static unregisterPending(requestId) {
        if (global._pendingMessages) {
            global._pendingMessages.delete(requestId);
        }
    }
}

Message.setupGlobalHandler();

module.exports = Message;