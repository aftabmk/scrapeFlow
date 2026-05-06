'use strict';

class BrowserHealth {
  _startHealthChecks() {
    this._browserHealthTimer = setInterval(
      () => this._browserHealthCheck().catch((e) =>
        console.error('[Browser] browserHealthCheck error:', e.message)),
      this._opts.browserHealthIntervalMs,
    );

    this._tabHealthTimer = setInterval(
      () => this._tabHealthCheck().catch((e) =>
        console.error('[Browser] tabHealthCheck error:', e.message)),
      this._opts.tabHealthIntervalMs,
    );
  }

  _stopHealthChecks() {
    if (this._browserHealthTimer) {
      clearInterval(this._browserHealthTimer);
      this._browserHealthTimer = null;
    }
    if (this._tabHealthTimer) {
      clearInterval(this._tabHealthTimer);
      this._tabHealthTimer = null;
    }
  }

  async _browserHealthCheck() {
    const elapsed = Date.now() - (this._lastHealthyAt?.getTime() ?? 0);
    if (elapsed > this._opts.browserHealthIntervalMs * 2) {
      console.warn('[Browser] health: possible hung check detected — restarting');
      return this._restart();
    }

    if (!this._browser?.isConnected()) {
      console.warn('[Browser] health: browser disconnected — restarting');
      return this._restart();
    }

    try {
      const memoryMb = await this._getChromiumMemoryMb();
      if (memoryMb !== null && memoryMb > this._opts.maxMemoryMb) {
        console.warn(`[Browser] health: memory ${memoryMb}MB exceeds ${this._opts.maxMemoryMb}MB — restarting`);
        return this._restart();
      }
    } catch (err) {
      console.warn('[Browser] health: memory check failed —', err.message);
    }

    this._lastHealthyAt = new Date();
  }

  async _tabHealthCheck() {
    const entries = this._cache.entries();
    const toEvict = [];

    for (const { name, tab } of entries) {
      if (!tab.checkAlive()) { toEvict.push({ name, reason: 'closed' }); continue; }
      if (tab.isStale())     { toEvict.push({ name, reason: 'stale'  }); continue; }
      if (tab.lastError?.includes('timed out')) {
        const alive = await tab.ping();
        if (!alive) toEvict.push({ name, reason: 'ping-failed' });
      }
    }

    for (const { name, reason } of toEvict) {
      console.info(`[Browser] tabHealthCheck: evicting tab "${name}" (${reason})`);
      await this._deleteTab(name);
    }
  }

  async _getChromiumMemoryMb() {
    if (process.platform !== 'linux') return null;
    try {
      const { execFile } = require('child_process');
      const { promisify } = require('util');
      const execFileAsync = promisify(execFile);
      const proc = this._browser?.process();
      if (!proc?.pid) return null;
      const { stdout } = await execFileAsync('cat', [`/proc/${proc.pid}/status`]);
      const match = stdout.match(/VmRSS:\s+(\d+)\s+kB/);
      return match ? Math.round(parseInt(match[1], 10) / 1024) : null;
    } catch {
      return null;
    }
  }
}

module.exports = { BrowserHealth };