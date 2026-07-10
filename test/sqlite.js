// test-sqlite.js
const SQLiteServer = require('./sqlite-server/server');

console.log('Testing SQLite Server...');

const server = new SQLiteServer({
  dbPath: './data/queue.db',
  writeWorkers: 1,
  readWorkers: 2
});

// ✅ Use a Promise to handle the ready event
const readyPromise = new Promise((resolve) => {
  server.on('ready', (info) => {
    console.log('✅ SQLite Server ready!', info);
    resolve(info);
  });
});

server.on('error', (err) => {
  console.error('❌ SQLite Server error:', err);
  process.exit(1);
});

// ✅ Start server and wait for ready
Promise.race([
  readyPromise,
  new Promise((_, reject) => 
    setTimeout(() => reject(new Error('Timeout waiting for ready')), 10000)
  )
])
.then((info) => {
  console.log('✅ Test passed!');
  console.log('Server info:', info);
  process.exit(0);
})
.catch((err) => {
  console.error('❌ Test failed:', err.message);
  process.exit(1);
});

// Start the server
server.start().catch((err) => {
  console.error('❌ Failed to start server:', err);
  process.exit(1);
});