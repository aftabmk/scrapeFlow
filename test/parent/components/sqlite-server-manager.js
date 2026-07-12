// parent/components/sqlite-server-manager.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class SQLiteServerManager extends EventEmitter {
    constructor(options = {}) {
        super();
        this.sqliteServer = null;
        this.isRunning = false;
        this.isReady = false;
        this.dbPath = options.dbPath || './data/queue.db';
        this.readWorkers = options.readWorkers || 3;
        this.writeWorkers = options.writeWorkers || 1;
        this.startTimeout = options.startTimeout || 10000;
        this.restartDelay = options.restartDelay || 2000;
        this.queueNames = options.queueNames || ['analyzer', 'browser', 'exporter', 'job-submitter'];
        this.attempts = 0;
        this.maxAttempts = options.maxAttempts || 3;
        this.tablesCreated = false;
    }

    start() {
        console.log('[SQLiteServerManager] 🚀 Starting SQLite Server...');
        this.attempts++;
        this.isRunning = true;
        
        return new Promise((resolve, reject) => {
            const serverPath = path.join(__dirname, '../../sqlite-server/index.js');
            const queueArg = this.queueNames.join(',');
            
            this.sqliteServer = fork(serverPath, [
                `--db-path=${this.dbPath}`,
                `--read-workers=${this.readWorkers}`,
                `--write-workers=${this.writeWorkers}`,
                `--queues=${queueArg}`  // ✅ Pass queue names as arg
            ], {
                stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
                execArgv: ['--experimental-sqlite'],
                env: {
                    ...process.env,
                    DB_PATH: this.dbPath,
                    READ_WORKERS: this.readWorkers,
                    WRITE_WORKERS: this.writeWorkers,
                    QUEUES: queueArg
                }
            });

            console.log(`[SQLiteServerManager] SQLite Server PID: ${this.sqliteServer.pid} (attempt ${this.attempts})`);
            console.log(`[SQLiteServerManager] 📋 Queues: ${queueArg}`);

            let resolved = false;
            let startTime = Date.now();

            // Periodic logging every 5 seconds
            const logInterval = setInterval(() => {
                if (!resolved) {
                    const elapsed = Math.floor((Date.now() - startTime) / 1000);
                    console.log(`[SQLiteServerManager] ⏳ Waiting for SQLite Server... (${elapsed}s elapsed)`);
                }
            }, 5000);

            // Handle messages
            this.sqliteServer.on('message', (msg) => {
                if (msg.type === 'SQLITE_READY') {
                    this.isReady = true;
                    console.log('[SQLiteServerManager] ✅ SQLite Server ready');
                    this.emit('ready', msg);
                }
                
                if (msg.type === 'ALL_TABLES_CREATED') {
                    this.tablesCreated = true;
                    console.log('[SQLiteServerManager] ✅ ALL tables created');
                    this.emit('allTablesCreated', msg);
                }
                
                if (msg.type === 'SQLITE_RESPONSE') {
                    // Forward to parent
                    this.emit('response', msg);
                }
            });

            this.sqliteServer.on('error', (err) => {
                console.error('[SQLiteServerManager] SQLite Server error:', err);
                clearInterval(logInterval);
                this.isRunning = false;
                if (!resolved) {
                    resolved = true;
                    reject(err);
                }
            });

            this.sqliteServer.on('exit', (code, signal) => {
                console.log(`[SQLiteServerManager] SQLite Server exited with code ${code}, signal ${signal}`);
                clearInterval(logInterval);
                this.isRunning = false;
                this.isReady = false;
                
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
                
                if (!resolved) {
                    resolved = true;
                    reject(new Error(`SQLite Server exited with code ${code}`));
                }
            });

            // Timeout
            const timeout = setTimeout(() => {
                if (!resolved && !this.tablesCreated) {
                    resolved = true;
                    clearInterval(logInterval);
                    console.error('[SQLiteServerManager] ❌ SQLite Server startup timeout');
                    this.sqliteServer.kill();
                    this.isRunning = false;
                    reject(new Error('SQLite Server startup timeout'));
                }
            }, this.startTimeout);

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

    isRunning() {
        return this.isRunning && this.sqliteServer !== null && this.sqliteServer.connected;
    }

    isReady() {
        return this.isReady;
    }

    areTablesCreated() {
        return this.tablesCreated;
    }

    shutdown() {
        console.log('[SQLiteServerManager] 🛑 Shutting down SQLite Server...');
        this.isRunning = false;
        this.isReady = false;

        if (this.sqliteServer) {
            try {
                this.sqliteServer.send({ type: 'SHUTDOWN' });
            } catch (err) {}

            return new Promise((resolve) => {
                this.sqliteServer.on('exit', resolve);
                setTimeout(resolve, 3000);
            });
        }
        
        console.log('[SQLiteServerManager] ✅ SQLite Server shutdown complete');
        this.emit('shutdown');
        return Promise.resolve();
    }
}

module.exports = SQLiteServerManager;