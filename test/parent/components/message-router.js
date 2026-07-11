// parent/components/message-router.js
const { EventEmitter } = require('events');

class MessageRouter extends EventEmitter {
    constructor(options = {}) {
        super();
        this.handlers = new Map();
        this.defaultHandler = null;
    }

    /**
     * Register a handler for a message type
     */
    register(type, handler) {
        this.handlers.set(type, handler);
        return this;
    }

    /**
     * Register default handler for unregistered message types
     */
    setDefaultHandler(handler) {
        this.defaultHandler = handler;
        return this;
    }

    /**
     * Route a message to the appropriate handler
     */
    route(message, processInfo) {
        if (!message || !message.type) {
            console.warn('[MessageRouter] ⚠️ Message without type received');
            return false;
        }

        const handler = this.handlers.get(message.type);
        if (handler) {
            try {
                handler(message, processInfo);
                this.emit('routed', { message, processInfo, handler: message.type });
                return true;
            } catch (error) {
                console.error(`[MessageRouter] ❌ Handler for ${message.type} failed:`, error.message);
                this.emit('error', { message, processInfo, error });
                return false;
            }
        } else if (this.defaultHandler) {
            try {
                this.defaultHandler(message, processInfo);
                this.emit('routed', { message, processInfo, handler: 'default' });
                return true;
            } catch (error) {
                console.error(`[MessageRouter] ❌ Default handler failed:`, error.message);
                this.emit('error', { message, processInfo, error });
                return false;
            }
        } else {
            console.warn(`[MessageRouter] ⚠️ No handler for message type: ${message.type}`);
            this.emit('unhandled', { message, processInfo });
            return false;
        }
    }

    /**
     * Remove a handler
     */
    unregister(type) {
        this.handlers.delete(type);
        return this;
    }

    /**
     * Get all registered handler types
     */
    getHandlerTypes() {
        return Array.from(this.handlers.keys());
    }

    /**
     * Clear all handlers
     */
    clear() {
        this.handlers.clear();
        this.defaultHandler = null;
        return this;
    }
}

module.exports = MessageRouter;