// performance/performance-monitor.js
const { performance } = require('perf_hooks');
const v8 = require('v8');
const fs = require('fs');
const path = require('path');
const { fork } = require('child_process');

// ============================================================
// CONFIGURATION
// ============================================================

const DEFAULT_CONFIG = {
    enabled: process.env.PROFILE === 'true' || false,
    logInterval: 10000,
    slowThreshold: 100,
    memoryThreshold: 1024,
    eventLoopThreshold: 50,
    cpuThreshold: 80,
    maxSamples: 1000,
    outputDir: './performance/metrics',
    outputFile: 'perf-report.json',
    logToFile: true,
    logToConsole: true
};

// ============================================================
// PERFORMANCE MONITOR CLASS
// ============================================================

class PerformanceMonitor {
    constructor(config = {}) {
        this.config = { ...DEFAULT_CONFIG, ...config };
        this.enabled = this.config.enabled;

        // ✅ Create output directory
        this._ensureOutputDir();

        // Metrics storage
        this.metrics = new Map();
        this.timers = new Map();
        this.samples = [];
        this.startTime = Date.now();

        // Event loop stats
        this.eventLoopStats = {
            min: Infinity,
            max: 0,
            avg: 0,
            count: 0,
            total: 0,
            blocked: 0
        };

        // CPU stats
        this.cpuUsage = {
            user: 0,
            system: 0,
            lastCheck: Date.now(),
            maxUser: 0,
            maxSystem: 0,
            samples: []
        };

        // Timers
        this.timersList = {
            memory: null,
            log: null,
            cpu: null,
            eventLoop: null
        };

        // IPC tracking
        this.ipcStats = {
            sent: 0,
            received: 0,
            sentBytes: 0,
            receivedBytes: 0
        };

        // Process tracking
        this.processStats = {
            forked: 0,
            exited: 0,
            restarted: 0
        };

        if (this.enabled) {
            this._printHeader();
            this._startMonitoring();
            this._patchIPC();
            this._patchFork();
        }
    }

    // ============================================================
    // DIRECTORY MANAGEMENT
    // ============================================================

    _ensureOutputDir() {
        const dir = this.config.outputDir;
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
            console.log(`📊 Created metrics directory: ${dir}`);
        }
    }

    _getOutputPath(filename) {
        return path.join(this.config.outputDir, filename);
    }

    // ============================================================
    // HEADER
    // ============================================================

    _printHeader() {
        console.log('\n📊 ==================================');
        console.log('📊 PERFORMANCE MONITORING ENABLED');
        console.log(`📊 PID: ${process.pid}`);
        console.log(`📊 Slow Threshold: ${this.config.slowThreshold}ms`);
        console.log(`📊 Memory Threshold: ${this.config.memoryThreshold}MB`);
        console.log(`📊 Event Loop Threshold: ${this.config.eventLoopThreshold}ms`);
        console.log(`📊 CPU Threshold: ${this.config.cpuThreshold}%`);
        console.log(`📊 Log Interval: ${this.config.logInterval}ms`);
        console.log(`📊 Output Directory: ${this.config.outputDir}`);
        console.log('📊 ==================================\n');
    }

    // ============================================================
    // TIMING API
    // ============================================================

    start(name, metadata = {}) {
        if (!this.enabled) return null;

        const id = `${name}_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;
        const entry = {
            id,
            name,
            metadata,
            startTime: performance.now(),
            startMemory: process.memoryUsage(),
            startCpu: process.cpuUsage(),
            timestamp: Date.now()
        };

        this.timers.set(id, entry);
        return id;
    }

    end(id, additional = {}) {
        if (!this.enabled || !this.timers.has(id)) return null;

        const entry = this.timers.get(id);
        const endTime = performance.now();
        const duration = endTime - entry.startTime;

        const result = {
            ...entry,
            duration,
            endTime,
            endMemory: process.memoryUsage(),
            cpuDelta: process.cpuUsage(entry.startCpu),
            memoryDelta: {
                heapUsed: process.memoryUsage().heapUsed - entry.startMemory.heapUsed,
                rss: process.memoryUsage().rss - entry.startMemory.rss,
                external: process.memoryUsage().external - entry.startMemory.external
            },
            ...additional
        };

        this.timers.delete(id);
        this._recordMetric(result);

        if (duration > this.config.slowThreshold) {
            this._logSlow(result);
        }

        return result;
    }

    async time(name, fn, metadata = {}) {
        const id = this.start(name, metadata);
        try {
            const result = await fn();
            this.end(id);
            return result;
        } catch (error) {
            this.end(id, { error: error.message });
            throw error;
        }
    }

    timeSync(name, fn, metadata = {}) {
        const id = this.start(name, metadata);
        try {
            const result = fn();
            this.end(id);
            return result;
        } catch (error) {
            this.end(id, { error: error.message });
            throw error;
        }
    }

    // ============================================================
    // METRIC RECORDING
    // ============================================================

    _recordMetric(entry) {
        if (!this.metrics.has(entry.name)) {
            this.metrics.set(entry.name, {
                name: entry.name,
                count: 0,
                totalTime: 0,
                minTime: Infinity,
                maxTime: 0,
                avgTime: 0,
                totalMemory: 0,
                maxMemory: 0,
                samples: []
            });
        }

        const metric = this.metrics.get(entry.name);
        metric.count++;
        metric.totalTime += entry.duration;
        metric.minTime = Math.min(metric.minTime, entry.duration);
        metric.maxTime = Math.max(metric.maxTime, entry.duration);
        metric.avgTime = metric.totalTime / metric.count;
        metric.totalMemory += entry.memoryDelta.heapUsed;
        metric.maxMemory = Math.max(metric.maxMemory, entry.memoryDelta.heapUsed);

        if (metric.samples.length < 50) {
            metric.samples.push({
                duration: entry.duration,
                memory: entry.memoryDelta,
                timestamp: entry.timestamp,
                metadata: entry.metadata
            });
        }

        this.samples.push({
            name: entry.name,
            duration: entry.duration,
            memory: entry.memoryDelta,
            cpu: entry.cpuDelta,
            timestamp: entry.timestamp
        });

        if (this.samples.length > this.config.maxSamples) {
            this.samples = this.samples.slice(-this.config.maxSamples);
        }
    }

    // ============================================================
    // MONITORING
    // ============================================================

    _startMonitoring() {
        this._startMemoryMonitor();
        this._startLogMonitor();
        this._startCPUMonitor();
        this._startEventLoopMonitor();
    }

    _startMemoryMonitor() {
        this.timersList.memory = setInterval(() => {
            const mem = process.memoryUsage();
            const heapUsedMB = mem.heapUsed / 1024 / 1024;
            const rssMB = mem.rss / 1024 / 1024;

            if (heapUsedMB > this.config.memoryThreshold) {
                this._logMemory(mem, heapUsedMB, rssMB);
            }

            this._updateMemoryStats(mem);
        }, 5000);
    }

    _startLogMonitor() {
        this.timersList.log = setInterval(() => {
            this._logSummary();
        }, this.config.logInterval);
    }

    _startCPUMonitor() {
        this.timersList.cpu = setInterval(() => {
            const cpu = process.cpuUsage();
            const now = Date.now();
            const elapsed = (now - this.cpuUsage.lastCheck) / 1000;

            if (elapsed > 0) {
                const userPercent = ((cpu.user - this.cpuUsage.user) / 1000 / elapsed) * 100;
                const systemPercent = ((cpu.system - this.cpuUsage.system) / 1000 / elapsed) * 100;
                const totalPercent = userPercent + systemPercent;

                this.cpuUsage.user = cpu.user;
                this.cpuUsage.system = cpu.system;
                this.cpuUsage.lastCheck = now;
                this.cpuUsage.maxUser = Math.max(this.cpuUsage.maxUser, userPercent);
                this.cpuUsage.maxSystem = Math.max(this.cpuUsage.maxSystem, systemPercent);

                this.cpuUsage.samples.push({
                    user: userPercent,
                    system: systemPercent,
                    total: totalPercent,
                    timestamp: Date.now()
                });

                if (this.cpuUsage.samples.length > 100) {
                    this.cpuUsage.samples = this.cpuUsage.samples.slice(-100);
                }

                if (totalPercent > this.config.cpuThreshold) {
                    this._logCPU(userPercent, systemPercent, totalPercent);
                }
            }
        }, 3000);
    }

    _startEventLoopMonitor() {
        this.timersList.eventLoop = setInterval(() => {
            const start = performance.now();
            setImmediate(() => {
                const delay = performance.now() - start;
                this.eventLoopStats.count++;
                this.eventLoopStats.total += delay;
                this.eventLoopStats.min = Math.min(this.eventLoopStats.min, delay);
                this.eventLoopStats.max = Math.max(this.eventLoopStats.max, delay);
                this.eventLoopStats.avg = this.eventLoopStats.total / this.eventLoopStats.count;

                if (delay > this.config.eventLoopThreshold) {
                    this.eventLoopStats.blocked++;
                    this._logEventLoop(delay);
                }
            });
        }, 1000);
    }

    // ============================================================
    // LOGGING
    // ============================================================

    _logSlow(entry) {
        if (!this.config.logToConsole) return;

        console.log(`⚠️ [PERF] SLOW: ${entry.name} took ${entry.duration.toFixed(2)}ms`);
        console.log(`   └─ Memory: +${(entry.memoryDelta.heapUsed / 1024 / 1024).toFixed(2)}MB`);
        if (entry.metadata && Object.keys(entry.metadata).length > 0) {
            console.log(`   └─ Metadata:`, entry.metadata);
        }
    }

    _logMemory(mem, heapUsedMB, rssMB) {
        if (!this.config.logToConsole) return;

        console.log(`⚠️ [PERF] Memory high: ${heapUsedMB.toFixed(2)}MB (RSS: ${rssMB.toFixed(2)}MB)`);
        console.log(`   └─ Heap Used: ${heapUsedMB.toFixed(2)}MB`);
        console.log(`   └─ Heap Total: ${(mem.heapTotal / 1024 / 1024).toFixed(2)}MB`);
        console.log(`   └─ External: ${(mem.external / 1024 / 1024).toFixed(2)}MB`);
        console.log(`   └─ Array Buffers: ${(mem.arrayBuffers / 1024 / 1024).toFixed(2)}MB`);

        if (this.timers.size > 0) {
            const activeNames = Array.from(this.timers.values()).map(t => t.name);
            console.log(`   └─ Active timers: ${this.timers.size} (${activeNames.join(', ')})`);
        }
    }

    _logCPU(userPercent, systemPercent, totalPercent) {
        if (!this.config.logToConsole) return;

        console.log(`⚠️ [PERF] High CPU: ${totalPercent.toFixed(1)}% (User: ${userPercent.toFixed(1)}%, System: ${systemPercent.toFixed(1)}%)`);
    }

    _logEventLoop(delay) {
        if (!this.config.logToConsole) return;

        console.log(`⚠️ [PERF] Event loop blocked: ${delay.toFixed(2)}ms`);
        console.log(`   └─ Active timers: ${this.timers.size}`);
    }

    // ============================================================
    // PATCHING
    // ============================================================

    _patchIPC() {
        const originalSend = process.send;
        if (!originalSend) return;

        const perf = this;
        process.send = function(message) {
            const size = JSON.stringify(message).length;
            const id = perf.start('IPC.send', { type: message?.type, size });
            try {
                const result = originalSend.apply(this, arguments);
                perf.end(id);
                perf.ipcStats.sent++;
                perf.ipcStats.sentBytes += size;
                return result;
            } catch (error) {
                perf.end(id, { error: error.message });
                throw error;
            }
        };

        const originalOn = process.on;
        process.on = function(event, listener) {
            if (event === 'message') {
                const wrapped = function(message) {
                    const size = JSON.stringify(message).length;
                    perf.ipcStats.received++;
                    perf.ipcStats.receivedBytes += size;
                    if (perf.ipcStats.received % 100 === 0) {
                        perf._logIPC();
                    }
                    return listener(message);
                };
                return originalOn.call(this, event, wrapped);
            }
            return originalOn.call(this, event, listener);
        };
    }

    _patchFork() {
        const perf = this;
        const originalFork = fork;

        const childProcess = require('child_process');
        childProcess.fork = function(modulePath, args, options) {
            const id = perf.start('Process.fork', { module: modulePath });
            try {
                const child = originalFork(modulePath, args, options);
                perf.end(id);
                perf.processStats.forked++;

                child.on('exit', () => {
                    perf.processStats.exited++;
                });

                return child;
            } catch (error) {
                perf.end(id, { error: error.message });
                throw error;
            }
        };
    }

    _logIPC() {
        if (!this.config.logToConsole) return;

        console.log(`📊 [PERF] IPC: ${this.ipcStats.sent} sent (${(this.ipcStats.sentBytes / 1024).toFixed(2)}KB), ${this.ipcStats.received} received (${(this.ipcStats.receivedBytes / 1024).toFixed(2)}KB)`);
    }

    // ============================================================
    // STATS UPDATE
    // ============================================================

    _updateMemoryStats(mem) {
        // Can store memory stats if needed
    }

    // ============================================================
    // SUMMARY
    // ============================================================

    _logSummary() {
        const summary = this.getSummary();

        console.log('\n📊 ===== PERFORMANCE SUMMARY =====');
        console.log(`📊 Uptime: ${((Date.now() - this.startTime) / 1000).toFixed(0)}s`);
        console.log(`📊 Total Functions Tracked: ${summary.totalFunctions}`);
        console.log(`📊 Total Calls: ${summary.totalCalls}`);
        console.log(`📊 Total Time: ${(summary.totalTime / 1000).toFixed(2)}s`);

        const mem = process.memoryUsage();
        console.log(`📊 Memory: Heap ${(mem.heapUsed / 1024 / 1024).toFixed(2)}MB / ${(mem.heapTotal / 1024 / 1024).toFixed(2)}MB (RSS: ${(mem.rss / 1024 / 1024).toFixed(2)}MB)`);

        if (this.eventLoopStats.count > 0) {
            console.log(`📊 Event Loop: Min ${this.eventLoopStats.min.toFixed(2)}ms, Max ${this.eventLoopStats.max.toFixed(2)}ms, Avg ${this.eventLoopStats.avg.toFixed(2)}ms`);
            console.log(`📊 Event Loop Blocks: ${this.eventLoopStats.blocked}`);
        }

        console.log(`📊 IPC: ${this.ipcStats.sent} sent, ${this.ipcStats.received} received`);
        console.log(`📊 Processes: ${this.processStats.forked} forked, ${this.processStats.exited} exited`);

        console.log('\n📊 Top 10 Functions by Total Time:');
        summary.topByTotalTime.slice(0, 10).forEach((m, i) => {
            const percent = (m.totalTime / summary.totalTime * 100).toFixed(1);
            console.log(`   ${i + 1}. ${m.name}: ${(m.totalTime / 1000).toFixed(2)}s (${m.count} calls, ${percent}%, avg: ${m.avgTime.toFixed(2)}ms)`);
        });

        console.log('\n📊 Top 5 Functions by Avg Time:');
        summary.topByAvgTime.slice(0, 5).forEach((m, i) => {
            console.log(`   ${i + 1}. ${m.name}: ${m.avgTime.toFixed(2)}ms (${m.count} calls)`);
        });

        console.log('📊 =================================\n');
    }

    // ============================================================
    // REPORTING
    // ============================================================

    getSummary() {
        const metrics = Array.from(this.metrics.values());
        const totalTime = metrics.reduce((sum, m) => sum + m.totalTime, 0);

        return {
            totalFunctions: metrics.length,
            totalCalls: metrics.reduce((sum, m) => sum + m.count, 0),
            totalTime: totalTime,
            topByTotalTime: [...metrics].sort((a, b) => b.totalTime - a.totalTime),
            topByAvgTime: [...metrics].sort((a, b) => b.avgTime - a.avgTime),
            topByCount: [...metrics].sort((a, b) => b.count - a.count),
            eventLoop: this.eventLoopStats,
            ipc: this.ipcStats,
            processes: this.processStats,
            cpu: {
                maxUser: this.cpuUsage.maxUser,
                maxSystem: this.cpuUsage.maxSystem,
                samples: this.cpuUsage.samples.slice(-10)
            },
            memory: process.memoryUsage(),
            uptime: Date.now() - this.startTime
        };
    }

    writeReport() {
        const report = {
            summary: this.getSummary(),
            samples: this.samples.slice(-500),
            timestamp: new Date().toISOString(),
            config: this.config,
            pid: process.pid,
            nodeVersion: process.version,
            platform: process.platform
        };

        const outputPath = this._getOutputPath(this.config.outputFile);
        fs.writeFileSync(outputPath, JSON.stringify(report, null, 2));
        console.log(`📊 Report written to: ${outputPath}`);
    }

    // ============================================================
    // TRACKING HELPERS
    // ============================================================

    trackMethod(obj, methodName) {
        const original = obj[methodName];
        const perf = this;

        obj[methodName] = function(...args) {
            const id = perf.start(`${obj.constructor.name}.${methodName}`, { args: args.length });
            try {
                const result = original.apply(this, args);
                if (result && typeof result.then === 'function') {
                    return result.then(val => {
                        perf.end(id);
                        return val;
                    }).catch(err => {
                        perf.end(id, { error: err.message });
                        throw err;
                    });
                }
                perf.end(id);
                return result;
            } catch (error) {
                perf.end(id, { error: error.message });
                throw error;
            }
        };
    }

    trackAllMethods(obj, filter = null) {
        const proto = Object.getPrototypeOf(obj);
        const methods = Object.getOwnPropertyNames(proto)
            .filter(name => {
                if (name === 'constructor') return false;
                if (typeof proto[name] !== 'function') return false;
                if (filter && !filter(name)) return false;
                return true;
            });

        for (const method of methods) {
            this.trackMethod(obj, method);
        }
    }

    // ============================================================
    // SHUTDOWN
    // ============================================================

    stop() {
        console.log('\n📊 Stopping performance monitor...');
        this.enabled = false;

        // Clear all timers
        for (const [key, timer] of Object.entries(this.timersList)) {
            if (timer) {
                clearInterval(timer);
                this.timersList[key] = null;
            }
        }

        // Write final report
        this._logSummary();
        this.writeReport();

        console.log('📊 Performance monitor stopped');
    }
}

// ============================================================
// CREATE SINGLETON INSTANCE
// ============================================================

const perf = new PerformanceMonitor();

// ============================================================
// EXPORTS
// ============================================================

module.exports = perf;
module.exports.PerformanceMonitor = PerformanceMonitor;