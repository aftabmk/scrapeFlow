// Define your functions once (in Node.js scope)
class SocketBuilder {
  create = (port = 8080, id = 'default') => {
    const url = `ws://localhost:${port}/${id}`
    const socket = new WebSocket(url);

    socket.addEventListener('open', () => {
      console.log(`✅ Connected: ${id}`);
    });

    socket.addEventListener('message', (event) => {
      console.log('📩 Message:', event.data);
    });

    socket.addEventListener('error', (error) => {
      console.error('❌ Socket error:', error);
    });

    socket.addEventListener('close', (event) => {
      console.log(`🔒 Closed: ${event.code} ${event.reason}`);
    });

    window.socket = socket;
    return socket;
  };
}

class HTMLRequest {
  fetch = async (endpoint, extraHeaders = {}, { timeoutMs = 15000, signal } = {}) => {
    const controller = new AbortController();

    // If the caller passed an external signal, forward its abort to our controller
    const onExternalAbort = () => controller.abort(signal.reason);
    if (signal) {
      if (signal.aborted) controller.abort(signal.reason);
      else signal.addEventListener('abort', onExternalAbort, { once: true });
    }

    const timeoutId = setTimeout(() => {
      controller.abort(new DOMException('Request timed out', 'TimeoutError'));
    }, timeoutMs);

    try {
      console.log(`🌐 Fetching: ${endpoint}`);

      const headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/plain, */*',
        'User-Agent': navigator.userAgent,
        'Referer': location.href,
        'Origin': location.origin,
        ...extraHeaders
      };

      const response = await fetch(endpoint, {
        method: 'GET',
        headers,
        credentials: 'include',
        signal: controller.signal
      });

      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const data = await response.json();

      window.StorageBucket.set(new Date(Date.now()), { endpont: endpoint, data: data });

      window.socket?.send(JSON.stringify({
        type: 'success',
        endpoint,
        data,
        timestamp: new Date().toISOString(),
        pageUrl: location.href,
        pageId: window.pageId || 'default'
      }));

      return data;
    } 
    catch (error) {
      const isAbort = error.name === 'AbortError' || error.name === 'TimeoutError';
      const message = isAbort
        ? (controller.signal.reason?.message || 'Request aborted')
        : error.message;

      console.error('❌ Fetch error:', message);

      window.socket?.send(JSON.stringify({
        type: 'error',
        endpoint,
        message,
        aborted: isAbort,
        pageUrl: location.href,
        pageId: window.pageId || 'default'
      }));

      throw error;
    } 
    finally {
      clearTimeout(timeoutId);
      if (signal) signal.removeEventListener('abort', onExternalAbort);
    }
  };
}

class StorageBucket {
  constructor(dbName = 'StorageBucket', storeName = 'bucket') {
    this.dbName = dbName;
    this.storeName = storeName;
    this.db = null;
  }

  async init() {
    if (this.db) return;

    this.db = await new Promise((resolve, reject) => {
      const request = indexedDB.open(this.dbName, 1);

      request.onupgradeneeded = () => {
        const db = request.result;

        if (!db.objectStoreNames.contains(this.storeName)) {
          db.createObjectStore(this.storeName); // for appending newer data
        }
      };

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  async set(key, value) {
    await this.init();

    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(this.storeName, 'readwrite');
      const store = tx.objectStore(this.storeName);

      store.put(value, key);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async get(key) {
    await this.init();

    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(this.storeName, 'readonly');
      const store = tx.objectStore(this.storeName);

      const req = store.get(key);

      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async remove(key) {
    await this.init();

    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(this.storeName, 'readwrite');
      const store = tx.objectStore(this.storeName);

      store.delete(key);

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  async clear() {
    await this.init();

    return new Promise((resolve, reject) => {
      const tx = this.db.transaction(this.storeName, 'readwrite');
      const store = tx.objectStore(this.storeName);

      store.clear();

      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }
}

module.exports = { SocketBuilder , HTMLRequest, StorageBucket };