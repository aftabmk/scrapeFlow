// sqlite-server/index.js
const SQLiteServer = require('./server');

if (require.main === module) {
  console.log('[SQLite Server] Starting...');
  console.log('[SQLite Server] PID:', process.pid);
  
  // Create server
  const server = new SQLiteServer({
    dbPath: process.env.DB_PATH || './data/queue.db',
    writeWorkers: 1,
    readWorkers: parseInt(process.env.READ_WORKERS) || 3
  });

  // ✅ Set up ready handler BEFORE starting
  server.on('ready', (info) => {
    console.log('[SQLite Server] Ready event received');
    
    // Send via IPC to parent
    if (process.send) {
      process.send({ type: 'SQLITE_READY', ...info });
      console.log('[SQLite Server] ✅ SQLITE_READY sent via IPC');
    } else {
      console.log('[SQLite Server] ⚠️ No parent process (standalone mode)');
    }
  });

  // ✅ Start the server
  server.start()
    .then(() => {
      console.log('[SQLite Server] Server started successfully');
    })
    .catch((err) => {
      console.error('[SQLite Server] Failed to start:', err);
      process.exit(1);
    });

  // Handle shutdown messages
  process.on('message', async (message) => {
    if (message && message.type === 'SHUTDOWN') {
      console.log('[SQLite Server] Received shutdown command');
      await server.shutdown();
    }
  });

  process.on('SIGINT', async () => {
    console.log('[SQLite Server] SIGINT received');
    await server.shutdown();
  });

  process.on('SIGTERM', async () => {
    console.log('[SQLite Server] SIGTERM received');
    await server.shutdown();
  });

  process.on('uncaughtException', (err) => {
    console.error('[SQLite Server] Uncaught exception:', err);
    process.exit(1);
  });
}

module.exports = SQLiteServer;