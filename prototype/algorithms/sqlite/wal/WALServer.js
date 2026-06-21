'use strict';

const { DatabaseSync } = require('node:sqlite');
const { WebSocketServer } = require('ws');
const path = require('node:path');
const fs = require('node:fs');

/**
 * Default dbPath: <WalServer.js's own directory>/<name of the folder
 * that contains the script which was actually run>/wal.db
 *
 * Example: if `queues/index.js` runs `node queues/index.js`, and
 * WalServer.js lives at the project root, this resolves to
 * `<project root>/queues/wal.db` — so each calling queue folder gets
 * its own db file under WalServer's directory, without the caller
 * having to pass dbPath explicitly.
 */
function defaultDbPath() {
  const invokedScript = process.argv[1] || __filename;
  const callerFolderName = path.basename(path.dirname(invokedScript));
  return path.join(__dirname, callerFolderName, 'wal.db');
}

/**
 * WalServer
 *
 * Owns the ONE node:sqlite connection. Nothing else in the system
 * touches SQLite directly — every queue's WriteAheadLog talks to this
 * over WebSocket instead.
 */
class WalServer {
  constructor({ port = 8766, dbPath = defaultDbPath() } = {}) {
    this.port = port;
    this.dbPath = dbPath;
    this.db = null;
    this.wss = null;
  }

  /**
   * Starts listening. Resolves once successfully bound to the port and
   * the SQLite connection is open. Rejects if the port is already in
   * use (EADDRINUSE) or any other bind error — callers use this to
   * detect "someone else is already the server" vs a real failure.
   */
  start() {
    return new Promise((resolve, reject) => {
      this.wss = new WebSocketServer({ port: this.port });

      this.wss.once('listening', () => {
        // Only open the SQLite connection once we've actually won the
        // port — a losing process should never touch the DB file at all.
        fs.mkdirSync(path.dirname(this.dbPath), { recursive: true });
        this.db = new DatabaseSync(this.dbPath);
        this.db.exec('PRAGMA journal_mode = WAL');
        this.db.exec(`
          CREATE TABLE IF NOT EXISTS wal_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_id TEXT NOT NULL,
            entry TEXT NOT NULL
          );
          CREATE INDEX IF NOT EXISTS idx_queue_id ON wal_entries(queue_id, id);
        `);

        this._insertStmt = this.db.prepare('INSERT INTO wal_entries (queue_id, entry) VALUES (?, ?)');
        this._replayStmt = this.db.prepare('SELECT entry FROM wal_entries WHERE queue_id = ? ORDER BY id');

        this.wss.on('connection', (ws) => this._handleConnection(ws));

        console.log(`[WalServer] listening on ws://localhost:${this.port}`);
        resolve(this);
      });

      this.wss.once('error', (err) => {
        reject(err); // err.code === 'EADDRINUSE' when someone else already bound this port
      });
    });
  }

  _handleConnection(ws) {
    ws.on('message', (raw) => {
      let msg;
      try {
        msg = JSON.parse(raw.toString());
      } catch {
        ws.send(JSON.stringify({ id: null, ok: false, error: 'invalid JSON' }));
        return;
      }
      this._handleMessage(ws, msg);
    });
  }

  _handleMessage(ws, msg) {
    const { id, method, queueId, entry } = msg;

    try {
      if (method === 'append') {
        this._insertStmt.run(queueId, JSON.stringify(entry));
        ws.send(JSON.stringify({ id, ok: true, result: true }));
      } else if (method === 'replay') {
        const rows = this._replayStmt.all(queueId);
        const entries = rows.map((r) => JSON.parse(r.entry));
        ws.send(JSON.stringify({ id, ok: true, result: entries }));
      } else {
        ws.send(JSON.stringify({ id, ok: false, error: `unknown method: ${method}` }));
      }
    } catch (err) {
      ws.send(JSON.stringify({ id, ok: false, error: err.message }));
    }
  }

  close() {
    this.wss?.close();
    this.db?.close();
  }

  /**
   * Call this before constructing any queue, instead of new WalServer()
   * + start() directly. Tries to become the WAL server itself; if the
   * port is already taken (another queue process got there first),
   * that's treated as success too — it just means this process will
   * talk to that other process's server as a normal client.
   *
   * Either way, by the time this resolves, a WAL server is guaranteed
   * to be listening on the given port. Returns the WalServer instance
   * if THIS process is hosting it (so it can be closed later), or null
   * if another process is already hosting it (nothing for this process
   * to own/close).
   */
  static async ensure(options = {}) {
    const server = new WalServer(options);
    try {
      await server.start();
      console.log(`[WalServer] this process is hosting the server on port ${server.port}`);
      return server;
    } catch (err) {
      if (err.code === 'EADDRINUSE') {
        console.log(`[WalServer] already running on port ${server.port}, connecting as a client`);
        return null;
      }
      throw err; // any other startup error is a real problem, don't swallow it
    }
  }
}

module.exports = WalServer;