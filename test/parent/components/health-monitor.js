// parent/components/health-monitor.js
const { EventEmitter } = require('events');

class HealthMonitor extends EventEmitter {
    constructor(options = {}) {
        super();
        this.processManager = options.processManager;
        this.heartbeatTimeout = options.heartbeatTimeout || 15000;
        this.checkInterval = options.checkInterval || 2000;
        this.isRunning = true;
        this.timer = null;
        this.heartbeats = new Map();
        this.stats = {
            totalHeartbeats: 0,
            missedHeartbeats: 0,
            restartsTriggered: 0
        };
        this.onRestart = options.onRestart || null;
    }

    /**
     * Start monitoring
     */
    start() {
        console.log('[HealthMonitor] ✅ Starting heartbeat monitoring...');
        this.timer = setInterval(() => {
            this._checkHealth();
        }, this.checkInterval);
        this.isRunning = true;
        this.emit('started');
        return this;
    }

    /**
     * Update heartbeat for a process
     */
    updateHeartbeat(pid, stats) {
        this.heartbeats.set(pid, {
            lastHeartbeat: Date.now(),
            stats: stats || {},
            missedCount: 0
        });
        this.stats.totalHeartbeats++;
        this.emit('heartbeat', { pid, stats });
    }

    /**
     * Check health of all processes
     */
    _checkHealth() {
        if (!this.isRunning) return;

        const now = Date.now();
        const processes = this.processManager.getAllProcesses();
        
        for (const info of processes) {
            if (info.status !== 'running') continue;
            
            const heartbeat = this.heartbeats.get(info.pid);
            if (!heartbeat) {
                // No heartbeat yet, check if process is still alive
                if (info.isReady) {
                    console.warn(`[HealthMonitor] ⚠️ No heartbeat for process ${info.pid} (${info.type})`);
                    this._handleMissedHeartbeat(info);
                }
                continue;
            }

            const elapsed = now - heartbeat.lastHeartbeat;
            if (elapsed > this.heartbeatTimeout) {
                console.warn(`[HealthMonitor] ⏰ Process ${info.pid} (${info.type}) heartbeat timeout (${elapsed}ms)`);
                this._handleMissedHeartbeat(info);
            }
        }
    }

    /**
     * Handle missed heartbeat
     */
    _handleMissedHeartbeat(info) {
        this.stats.missedHeartbeats++;
        
        const heartbeat = this.heartbeats.get(info.pid);
        if (heartbeat) {
            heartbeat.missedCount++;
        }

        this.emit('missed', { pid: info.pid, type: info.type, info });

        if (this.onRestart) {
            this.onRestart(info);
        }
    }

    /**
     * Get process health status
     */
    getHealthStatus() {
        const status = {};
        for (const [pid, info] of this.processManager.processes) {
            const heartbeat = this.heartbeats.get(pid);
            status[pid] = {
                pid,
                type: info.type,
                status: info.status,
                isReady: info.isReady,
                lastHeartbeat: heartbeat ? heartbeat.lastHeartbeat : null,
                missedCount: heartbeat ? heartbeat.missedCount : 0,
                isHealthy: heartbeat ? (Date.now() - heartbeat.lastHeartbeat) < this.heartbeatTimeout : false
            };
        }
        return status;
    }

    /**
     * Get statistics
     */
    getStats() {
        return {
            ...this.stats,
            monitoredProcesses: this.heartbeats.size,
            isRunning: this.isRunning,
            checkInterval: this.checkInterval,
            heartbeatTimeout: this.heartbeatTimeout
        };
    }

    /**
     * Stop monitoring
     */
    stop() {
        console.log('[HealthMonitor] 🛑 Stopping heartbeat monitoring...');
        this.isRunning = false;
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
        this.emit('stopped');
        return this;
    }
}

module.exports = HealthMonitor;