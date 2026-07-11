// parent/components/sqlite-server-manager.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class SQLiteServerManager extends EventEmitter {
    constructor(options = {}) {
        super();
        this.sqliteServer = null;
        this._isRunning = false;  // ✅ Use private property
        this._isReady = false;
        this.dbPath = options.dbPath || './data/queue.db';
        this.readWorkers = options.readWorkers || 3;
        this.writeWorkers = options.writeWorkers || 1;
        this.startTimeout = options.startTimeout || 30000;
        this.restartDelay = options.restartDelay || 2000;
        this.attempts = 0;
        this.maxAttempts = options.maxAttempts || 3;
    }

    // ✅ Getter methods
    isRunning() {
        return this._isRunning && this.sqliteServer !== null && this.sqliteServer.connected;
    }

    isReady() {
        return this._isReady;
    }

    /**
     * Start SQLite Server
     */
    async start() {
        console.log('[SQLiteServerManager] 🚀 Starting SQLite Server...');
        this.attempts++;
        this._isRunning = true;
        
        return new Promise((resolve, reject) => {
            const serverPath = path.join(__dirname, '../../sqlite-server/index.js');
            
            this.sqliteServer = fork(serverPath, [], {
                stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
                execArgv: ['--experimental-sqlite'],
                env: {
                    ...process.env,
                    DB_PATH: this.dbPath,
                    READ_WORKERS: this.readWorkers,
                    WRITE_WORKERS: this.writeWorkers
                }
            });

            console.log(`[SQLiteServerManager] SQLite Server PID: ${this.sqliteServer.pid} (attempt ${this.attempts})`);

            let resolved = false;
            let startTime = Date.now();

            // Periodic logging
            const logInterval = setInterval(() => {
                if (!resolved) {
                    const elapsed = Math.floor((Date.now() - startTime) / 1000);
                    console.log(`[SQLiteServerManager] ⏳ Waiting for SQLite Server... (${elapsed}s elapsed)`);
                }
            }, 30000);

            // Handle ready message
            this.sqliteServer.on('message', (msg) => {
                if (msg.type === 'SQLITE_READY') {
                    if (!resolved) {
                        resolved = true;
                        this._isReady = true;
                        clearInterval(logInterval);
                        const elapsed = Math.floor((Date.now() - startTime) / 1000);
                        console.log(`[SQLiteServerManager] ✅ SQLite Server ready after ${elapsed}s`);
                        this.emit('ready', msg);
                        resolve(msg);
                    }
                }
            });

            // Handle errors
            this.sqliteServer.on('error', (err) => {
                console.error('[SQLiteServerManager] SQLite Server error:', err);
                clearInterval(logInterval);
                this._isRunning = false;
                if (!resolved) {
                    resolved = true;
                    reject(err);
                }
            });

            // Handle exit
            this.sqliteServer.on('exit', (code, signal) => {
                console.log(`[SQLiteServerManager] SQLite Server exited with code ${code}, signal ${signal}`);
                clearInterval(logInterval);
                this._isRunning = false;
                this._isReady = false;
                
                if (this._isRunning && resolved) {
                    if (this.attempts < this.maxAttempts) {
                        console.log(`[SQLiteServerManager] 🔄 Restarting SQLite Server (attempt ${this.attempts + 1}/${this.maxAttempts})...`);
                        setTimeout(() => {
                            this.start().catch(err => {
                                console.error('[SQLiteServerManager] ❌ Failed to restart:', err);
                            });
                        }, this.restartDelay);
                    } else {
                        console.error('[SQLiteServerManager] ❌ Max restart attempts reached');
                        this.emit('maxAttemptsReached', { attempts: this.attempts });
                    }
                }
                
                if (!resolved) {
                    resolved = true;
                    reject(new Error(`SQLite Server exited with code ${code}`));
                }
            });

            // Timeout
            const timeout = setTimeout(() => {
                if (!resolved) {
                    resolved = true;
                    clearInterval(logInterval);
                    console.error('[SQLiteServerManager] ❌ SQLite Server startup timeout');
                    this.sqliteServer.kill();
                    this._isRunning = false;
                    this._isReady = false;
                    reject(new Error('SQLite Server startup timeout'));
                }
            }, this.startTimeout);

            // Cleanup
            const cleanup = () => {
                clearTimeout(timeout);
                clearInterval(logInterval);
            };
            
            const origResolve = resolve;
            resolve = (value) => { cleanup(); origResolve(value); };
            const origReject = reject;
            reject = (value) => { cleanup(); origReject(value); };
        });
    }

    /**
     * Send message to SQLite Server
     */
    send(message) {
        if (this.sqliteServer && this.sqliteServer.connected) {
            try {
                this.sqliteServer.send(message);
                return true;
            } catch (error) {
                console.error('[SQLiteServerManager] Failed to send message:', error.message);
                return false;
            }
        }
        return false;
    }

    /**
     * Get process info
     */
    getProcessInfo() {
        if (this.sqliteServer) {
            return {
                pid: this.sqliteServer.pid,
                connected: this.sqliteServer.connected,
                isReady: this._isReady,
                isRunning: this._isRunning,
                dbPath: this.dbPath,
                attempts: this.attempts
            };
        }
        return null;
    }

    /**
     * Reset attempt counter
     */
    resetAttempts() {
        this.attempts = 0;
        return this;
    }

    /**
     * Shutdown
     */
    async shutdown() {
        console.log('[SQLiteServerManager] 🛑 Shutting down SQLite Server...');
        this._isRunning = false;
        this._isReady = false;

        if (this.sqliteServer) {
            try {
                this.sqliteServer.send({ type: 'SHUTDOWN' });
            } catch (err) {}

            await new Promise((resolve) => {
                this.sqliteServer.on('exit', resolve);
                setTimeout(resolve, 3000);
            });
        }

        console.log('[SQLiteServerManager] ✅ SQLite Server shutdown complete');
        this.emit('shutdown');
    }
}

module.exports = SQLiteServerManager;