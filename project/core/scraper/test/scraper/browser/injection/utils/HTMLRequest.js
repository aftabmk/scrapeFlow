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

module.exports = HTMLRequest;