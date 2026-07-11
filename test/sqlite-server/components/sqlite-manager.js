// sqlite-server/components/sqlite-manager.js
const { DatabaseSync } = require('node:sqlite');
const path = require('path');
const fs = require('fs');
const { EventEmitter } = require('events');

class SQLiteManager extends EventEmitter {
    constructor(options = {}) {
        super();
        this.dbPath = options.dbPath || './data/queue.db';
        this.db = null;
        this.queries = null;
        this.tables = new Map();
        this.isConnected = false;
    }

    /**
     * Initialize database connection
     */
    initialize() {
        console.log('[SQLiteManager] Initializing database...');
        
        const dir = path.dirname(this.dbPath);
        if (!fs.existsSync(dir)) {
            console.log(`[SQLiteManager] Creating directory: ${dir}`);
            fs.mkdirSync(dir, { recursive: true });
        }

        try {
            this.db = new DatabaseSync(this.dbPath);
            this.isConnected = true;
            console.log('[SQLiteManager] ✅ Database connection established');
        } catch (error) {
            console.error('[SQLiteManager] ❌ Failed to connect to database:', error);
            throw error;
        }

        // Enable WAL for durability
        this.db.exec('PRAGMA journal_mode=WAL');
        this.db.exec('PRAGMA synchronous=NORMAL');
        this.db.exec('PRAGMA busy_timeout=5000');

        // Create SQL Queries instance
        const SQLQueries = require('../sql-queries');
        this.queries = new SQLQueries(this.db);

        // Create tables
        this._createTables();

        console.log('[SQLiteManager] ✅ Database initialized');
        this.emit('ready', { dbPath: this.dbPath, isConnected: this.isConnected });
    }

    /**
     * Create all necessary tables
     */
    _createTables() {
        // Queue log table for recovery
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS queue_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                op TEXT NOT NULL,
                job_id TEXT NOT NULL,
                payload TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Dead letter table
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS dead_letter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                queue_name TEXT NOT NULL,
                job_id TEXT NOT NULL,
                payload TEXT,
                retries INTEGER DEFAULT 0,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        // Queue state tracking
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS queue_state (
                queue_name TEXT PRIMARY KEY,
                pending_count INTEGER DEFAULT 0,
                in_flight_count INTEGER DEFAULT 0,
                dead_letter_count INTEGER DEFAULT 0,
                last_updated DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        `);

        console.log('[SQLiteManager] ✅ Tables created');
    }

    /**
     * Get or create a queue table
     */
    getQueueTable(queueName) {
        if (!this.tables.has(queueName)) {
            const tableName = `queue_${queueName.replace(/[^a-zA-Z0-9_]/g, '_')}`;

            this.db.exec(`
                CREATE TABLE IF NOT EXISTS ${tableName} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_id TEXT UNIQUE NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT DEFAULT 'PENDING',
                    retries INTEGER DEFAULT 0,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            `);

            this.db.exec(`
                CREATE INDEX IF NOT EXISTS idx_${tableName}_status 
                ON ${tableName}(status, created_at)
            `);

            this.tables.set(queueName, tableName);
            console.log(`[SQLiteManager] ✅ Created table: ${tableName}`);
        }

        return this.tables.get(queueName);
    }

    /**
     * Get SQL queries instance
     */
    getQueries() {
        return this.queries;
    }

    /**
     * Get database instance
     */
    getDB() {
        return this.db;
    }

    /**
     * Check if connected
     */
    isConnected() {
        return this.isConnected;
    }

    /**
     * Close database connection
     */
    close() {
        if (this.db) {
            this.db.close();
            this.isConnected = false;
            console.log('[SQLiteManager] ✅ Database connection closed');
        }
    }
}

module.exports = SQLiteManager;