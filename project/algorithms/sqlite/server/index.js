const WALServer = require('./core/WALServer');

if (require.main === module) {
    new WALServer().listen();
}