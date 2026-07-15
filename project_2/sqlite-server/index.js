// sqlite-server/index.js
const SQLiteServer = require('./server');

// Parse command line arguments
const args = process.argv.slice(2);
const options = {
    dbPath: './data/queue.db',
    readWorkers: 3,
    writeWorkers: 1,
    queueNames: ['analyzer', 'browser', 'exporter', 'job-submitter']
};

for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--db-path=')) {
        options.dbPath = arg.split('=')[1];
    } else if (arg.startsWith('--read-workers=')) {
        options.readWorkers = parseInt(arg.split('=')[1]) || 3;
    } else if (arg.startsWith('--write-workers=')) {
        options.writeWorkers = parseInt(arg.split('=')[1]) || 1;
    } else if (arg.startsWith('--queues=')) {
        options.queueNames = arg.split('=')[1].split(',');
    }
}

if (require.main === module) {
    console.log('[SQLiteServer] 🚀 Starting SQLite Server process...');
    console.log(`[SQLiteServer] PID: ${process.pid}`);
    console.log(`[SQLiteServer] DB_PATH: ${options.dbPath}`);
    console.log(`[SQLiteServer] READ_WORKERS: ${options.readWorkers}`);
    console.log(`[SQLiteServer] WRITE_WORKERS: ${options.writeWorkers}`);
    console.log(`[SQLiteServer] QUEUES: ${options.queueNames.join(', ')}`);

    // ✅ Handle uncaught exceptions
    process.on('uncaughtException', (err) => {
        console.error('[SQLiteServer] Uncaught exception:', err.message);
        console.error('[SQLiteServer] Stack:', err.stack);
        process.exit(1);
    });

    process.on('unhandledRejection', (reason, promise) => {
        console.error('[SQLiteServer] Unhandled rejection:', reason);
        process.exit(1);
    });

    try {
        const server = new SQLiteServer(options);

        server.on('ready', (info) => {
            console.log('[SQLiteServer] ✅ Ready');
        });

        // At the top after server.start()
        server.start().then(() => {
            console.log('[SQLiteServer] ✅ Server running');
            console.log('[SQLiteServer] 📨 Sending signals to parent...');
        }).catch((err) => {
            console.error('[SQLiteServer] Failed to start:', err.message);
            console.error('[SQLiteServer] Stack:', err.stack);
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

    } catch (error) {
        console.error('[SQLiteServer] Fatal error:', error.message);
        console.error('[SQLiteServer] Stack:', error.stack);
        process.exit(1);
    }
}

module.exports = SQLiteServer;