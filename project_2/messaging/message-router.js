// messaging/message-router.js
const { EventEmitter } = require('events');
const Message = require('./message');
const { MessageTypes, MessageDestinations } = require('./message-types');

class MessageRouter extends EventEmitter {
    constructor(options = {}) {
        super();
        this.processManager = options.processManager;
        this.sqliteManager = options.sqliteManager;
        this.orchestrator = options.orchestrator;
        this.messageHandler = options.messageHandler || null;  // ✅ Added
        this.messageLog = [];
        this.maxLogSize = options.maxLogSize || 1000;
        this.routes = new Map();
        this.handlers = new Map();

        this._registerDefaultRoutes();
    }

    // === Configuration ===

    setMessageHandler(messageHandler) {
        this.messageHandler = messageHandler;
        return this;
    }

    setOrchestrator(orchestrator) {
        this.orchestrator = orchestrator;
        return this;
    }

    setSQLiteManager(sqliteManager) {
        this.sqliteManager = sqliteManager;
        return this;
    }

    setProcessManager(processManager) {
        this.processManager = processManager;
        return this;
    }

    registerRoute(type, handler) {
        this.handlers.set(type, handler);
        return this;
    }

    // === Route Message ===

    route(message) {
        const msg = message instanceof Message ? message : Message.from(message);

        this._logMessage(msg);

        // ✅ Message for orchestrator
        if (msg.to === MessageDestinations.ORCHESTRATOR) {
            return this._handleOrchestratorMessage(msg);
        }

        // ✅ Message for SQLite Server
        if (msg.to === MessageDestinations.SQLITE_SERVER) {
            return this._routeToSQLite(msg);
        }

        // ✅ Message for child process
        if (this.processManager) {
            return this._routeToChild(msg);
        }

        console.error(`[MessageRouter] ❌ No destination for message: ${msg.type} (to: ${msg.to})`);
        this.emit('error', { message: msg, error: 'No destination found' });
        return false;
    }

    // === Handle Orchestrator Messages ===

    _handleOrchestratorMessage(msg) {
        // ✅ Try using MessageHandler first (injected from orchestrator)
        if (this.messageHandler) {
            const handled = this.messageHandler.handle(msg);
            if (handled) {
                this.emit('routed', { message: msg, destination: 'orchestrator-handler' });
                return true;
            }
        }

        // ✅ Try internal handlers
        if (this.handlers.has(msg.type)) {
            const handler = this.handlers.get(msg.type);
            try {
                handler(msg);
                this.emit('routed', { message: msg, destination: 'orchestrator-router' });
                return true;
            } catch (error) {
                console.error(`[MessageRouter] ❌ Handler failed for ${msg.type}:`, error.message);
                this.emit('error', { message: msg, error });
                return false;
            }
        }

        // ✅ Try orchestrator's handleMessage method
        if (this.orchestrator && typeof this.orchestrator.handleMessage === 'function') {
            return this.orchestrator.handleMessage(msg);
        }

        // ❌ No handler found
        console.warn(`[MessageRouter] ⚠️ No handler for message type: ${msg.type} from ${msg.from}`);
        this.emit('unhandled', msg);
        return false;
    }

    // === Route to SQLite ===

    _routeToSQLite(msg) {
        if (!this.sqliteManager) {
            console.error('[MessageRouter] ❌ SQLiteManager not available');
            this.emit('error', { message: msg, error: 'SQLiteManager not available' });
            return false;
        }

        if (!this.sqliteManager.isRunning()) {
            console.error('[MessageRouter] ❌ SQLite Server not running');
            this.emit('error', { message: msg, error: 'SQLite Server not running' });
            return false;
        }

        const success = this.sqliteManager.send(msg.payload);
        if (success) {
            this.emit('routed', { message: msg, destination: 'sqlite-server' });
        }
        return success;
    }

    // === Route to Child ===

    _routeToChild(msg) {
        if (!this.processManager) {
            console.error('[MessageRouter] ❌ ProcessManager not available');
            this.emit('error', { message: msg, error: 'ProcessManager not available' });
            return false;
        }

        const targetProcess = this.processManager.getProcess(msg.to);
        if (!targetProcess) {
            console.error(`[MessageRouter] ❌ Target process not found: ${msg.to}`);
            this.emit('error', { message: msg, error: `Target process not found: ${msg.to}` });
            return false;
        }

        if (msg.send(targetProcess.child)) {
            this.emit('routed', { message: msg, destination: msg.to });
            return true;
        }

        return false;
    }

    // === Default Routes ===

    _registerDefaultRoutes() {
        // Can be extended by orchestrator
    }

    // === Logging ===

    _logMessage(msg) {
        const entry = {
            from: msg.from,
            to: msg.to,
            type: msg.type,
            requestId: msg.requestId,
            timestamp: Date.now()
        };

        this.messageLog.push(entry);

        if (this.messageLog.length > this.maxLogSize) {
            this.messageLog = this.messageLog.slice(-this.maxLogSize);
        }

        this.emit('messageLogged', entry);
    }

    getMessageLog() {
        return this.messageLog;
    }

    clearLog() {
        this.messageLog = [];
        return this;
    }

    getStats() {
        return {
            logSize: this.messageLog.length,
            registeredRoutes: this.handlers.size,
            maxLogSize: this.maxLogSize
        };
    }
}

module.exports = MessageRouter;