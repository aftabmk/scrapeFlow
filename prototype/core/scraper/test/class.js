// Define your functions once (in Node.js scope)
class SocketBuilder {
  create = (port = 8080, id = 'default') => {
    const socket = new WebSocket(`ws://localhost:${port}/${id}`);
    window.socket = socket;
    return socket;
  };
}

class HTMLRequest {
  fetch = async (endpoint, extraHeaders = {}) => {
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

      const response = await fetch(endpoint, {method: 'GET',headers,credentials: 'include'});

      if (!response.ok) throw new Error(`HTTP ${response.status}`);

      const data = await response.json();

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
      console.error('❌ Fetch error:', error.message);
      window.socket?.send(JSON.stringify({
        type: 'error',
        endpoint,
        message: error.message,
        pageUrl: location.href,
        pageId: window.pageId || 'default'
      }));
      throw error;
    }
  };
}

module.exports = { SocketBuilder , HTMLRequest };