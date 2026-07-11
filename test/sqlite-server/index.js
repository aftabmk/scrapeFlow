// sqlite-server/index.js
const SQLiteServer = require('./server');

if (require.main === module) {
    console.log('[SQLiteServer] 🚀 Starting...');

    const server = new SQLiteServer({
        dbPath: process.env.DB_PATH || './data/queue.db'
    });

    process.on('SIGINT', () => server.shutdown());
    process.on('SIGTERM', () => server.shutdown());

    console.log('[SQLiteServer] ✅ Ready');
}

module.exports = SQLiteServer;