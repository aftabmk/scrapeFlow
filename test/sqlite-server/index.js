// sqlite-server/index.js
const SQLiteServer = require('./server');

if (require.main === module) {
    console.log('[SQLiteServer] 🚀 Starting SQLite Server process...');
    console.log(`[SQLiteServer] PID: ${process.pid}`);
    console.log(`[SQLiteServer] DB_PATH: ${process.env.DB_PATH || './data/queue.db'}`);
    console.log(`[SQLiteServer] READ_WORKERS: ${process.env.READ_WORKERS || 3}`);
    console.log(`[SQLiteServer] WRITE_WORKERS: ${process.env.WRITE_WORKERS || 1}`);

    try {
        const server = new SQLiteServer({
            dbPath: process.env.DB_PATH || './data/queue.db',
            writeWorkers: parseInt(process.env.WRITE_WORKERS) || 1,
            readWorkers: parseInt(process.env.READ_WORKERS) || 3
        });

        server.on('ready', (info) => {
            console.log('[SQLiteServer] ✅ Ready signal sent to parent');
            if (process.send) {
                process.send({ type: 'SQLITE_READY', ...info });
            }
        });

        server.start().catch((err) => {
            console.error('[SQLiteServer] Failed to start:', err);
            process.exit(1);
        });

        process.on('message', async (message) => {
            if (message && message.type === 'SHUTDOWN') {
                console.log('[SQLiteServer] Received shutdown command');
                await server.shutdown();
            }
        });

        process.on('SIGINT', async () => {
            console.log('[SQLiteServer] SIGINT received');
            await server.shutdown();
        });

        process.on('SIGTERM', async () => {
            console.log('[SQLiteServer] SIGTERM received');
            await server.shutdown();
        });

        process.on('uncaughtException', (err) => {
            console.error('[SQLiteServer] Uncaught exception:', err);
            process.exit(1);
        });

    } catch (error) {
        console.error('[SQLiteServer] Fatal error:', error);
        process.exit(1);
    }
}

module.exports = SQLiteServer;