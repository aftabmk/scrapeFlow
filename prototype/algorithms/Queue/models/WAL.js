'use strict';

const WebSocket = require('ws');

const SERVER_URL = 'ws://localhost:8766';

/**
 * WriteAheadLog (WebSocket-backed)
 *
 * Same shape as the original file-based version: append(entry), replay().
 * constructor(filePath, queueId) — filePath kept for compatibility with
 * existing call sites (new WriteAheadLog(walPath)), queueId is the new
 * second argument scoping entries to this queue on the shared server.
 *
 * The server is assumed to already be running (independent process), so
 * the constructor doesn't need to be async — ws.send() before 'open'
 * just gets queued by the socket itself; the only thing we add is
 * buffering calls made before 'open' fires, then flushing them in order.
 */
class WriteAheadLog {
  constructor(queueId) {
    this.queueId = queueId;

    this._nextId = 1;
    this._pending = new Map(); // rpc id -> { resolve, reject }
    this._sendQueue = []; // messages waiting for the socket to open
    this._open = false;

    this._ws = new WebSocket(SERVER_URL);

    this._ws.on('open', () => {
      this._open = true;
      for (const raw of this._sendQueue) this._ws.send(raw);
      this._sendQueue = [];
    });

    this._ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      const pending = this._pending.get(msg.id);
      if (!pending) return;
      this._pending.delete(msg.id);
      if (msg.ok) pending.resolve(msg.result);
      else pending.reject(new Error(msg.error));
    });
  }

  _send(method, extra) {
    const id = this._nextId++;
    const payload = JSON.stringify({ id, method, queueId: this.queueId, ...extra });
    return new Promise((resolve, reject) => {
      this._pending.set(id, { resolve, reject });
      if (this._open) this._ws.send(payload);
      else this._sendQueue.push(payload);
    });
  }

  append(entry) {
    return this._send('append', { entry });
  }

  replay() {
    return this._send('replay', {});
  }

  clear() {
    // Not used by BaseQueue; kept as a no-op for API compatibility.
  }
}

module.exports = WriteAheadLog;