// messaging/message-handler.js
const Message = require('./message');
const { MessageTypes } = require('./message-types');

class MessageHandler {
    constructor() {
        this.handlers = new Map();
        this.middleware = [];
        this.fallback = null;
    }

    register(type, handler, priority = 0) {
        if (!this.handlers.has(type)) {
            this.handlers.set(type, []);
        }
        this.handlers.get(type).push({ handler, priority });
        this.handlers.get(type).sort((a, b) => b.priority - a.priority);
        return this;
    }

    use(middleware) {
        this.middleware.push(middleware);
        return this;
    }

    setFallback(handler) {
        this.fallback = handler;
        return this;
    }

    handle(message) {
        const msg = message instanceof Message ? message : Message.from(message);

        for (const middleware of this.middleware) {
            const result = middleware(msg);
            if (!result) {
                console.warn(`[MessageHandler] ⚠️ Middleware blocked message: ${msg.type}`);
                return false;
            }
        }

        const handlers = this.handlers.get(msg.type) || [];
        let handled = false;

        for (const { handler } of handlers) {
            try {
                handler(msg);
                handled = true;
            } catch (error) {
                console.error(`[MessageHandler] ❌ Handler failed for ${msg.type}:`, error.message);
            }
        }

        if (!handled && this.fallback) {
            try {
                this.fallback(msg);
                handled = true;
            } catch (error) {
                console.error(`[MessageHandler] ❌ Fallback handler failed:`, error.message);
            }
        }

        if (!handled) {
            console.warn(`[MessageHandler] ⚠️ No handler for message type: ${msg.type}`);
        }

        return handled;
    }

    unregister(type) {
        this.handlers.delete(type);
        return this;
    }

    clear() {
        this.handlers.clear();
        this.middleware = [];
        this.fallback = null;
        return this;
    }

    getTypes() {
        return Array.from(this.handlers.keys());
    }

    has(type) {
        return this.handlers.has(type);
    }
}

module.exports = MessageHandler;