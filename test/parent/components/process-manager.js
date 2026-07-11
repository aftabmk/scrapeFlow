// parent/components/process-manager.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class ProcessManager extends EventEmitter {
    constructor(options = {}) {
        super();
        this.processes = new Map();
        this.expectedProcesses = 0;
        this.readyProcesses = 0;
        this.allProcessesReady = false;
        this.restartDelay = options.restartDelay || 2000;
        this.isRunning = true;
        this.scriptMap = {
            browser: './child/browser.js',
            analyzer: './child/analyzer.js',
            exporter: './child/exporter.js',
            'job-submitter': './child/job-submitter.js'
        };
    }

    createProcess(options = {}) {
        const {
            type = 'browser',
            processingWorkers = 2,
            queueName = `${type}_queue`,
            sqliteIndex = 0,
            dbPath = './data/queue.db',
            args = []
        } = options;

        const scriptPath = this.scriptMap[type] || this.scriptMap.browser;
        const fullPath = path.join(__dirname, '../..', scriptPath);

        const child = fork(fullPath, [
            `--processing-workers=${processingWorkers}`,
            `--queue-name=${queueName}`,
            `--process-type=${type}`,
            `--sqlite-index=${sqliteIndex}`,
            `--db-path=${dbPath}`,
            ...args
        ], {
            stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
            execArgv: ['--experimental-sqlite']
        });

        const processInfo = {
            pid: child.pid,
            child,
            type,
            processingWorkers,
            queueName,
            sqliteIndex,
            dbPath,
            lastHeartbeat: Date.now(),
            status: 'starting',
            createdAt: Date.now(),
            restartCount: 0,
            isReady: false
        };

        child.on('message', (msg) => {
            this.emit('message', processInfo, msg);
        });

        child.on('exit', (code, signal) => {
            this.emit('exit', processInfo, code, signal);
        });

        child.on('error', (err) => {
            console.error(`[ProcessManager] Child ${child.pid} error:`, err);
            this.emit('exit', processInfo, -1, 'error');
        });

        this.processes.set(child.pid, processInfo);
        this.expectedProcesses++;

        return new Promise((resolve) => {
            const readyHandler = (msg) => {
                if (msg.type === 'ready' && msg.processType === type) {
                    processInfo.status = 'running';
                    processInfo.isReady = true;
                    this.readyProcesses++;
                    
                    console.log(`[ProcessManager] ✅ Process ${child.pid} (${type}) ready (${this.readyProcesses}/${this.expectedProcesses})`);
                    this.emit('processReady', { pid: child.pid, type, ...msg });
                    
                    this._checkAllProcessesReady();
                    resolve(processInfo);
                    child.off('message', readyHandler);
                }
            };
            
            child.on('message', readyHandler);
            
            setTimeout(() => {
                child.off('message', readyHandler);
                if (processInfo.status === 'starting') {
                    processInfo.status = 'timeout';
                    console.warn(`[ProcessManager] Process ${child.pid} (${type}) startup timeout`);
                    this.emit('processTimeout', { pid: child.pid, type });
                    resolve(processInfo);
                }
            }, 15000);
        });
    }

    _checkAllProcessesReady() {
        if (this.allProcessesReady) return;
        
        if (this.readyProcesses === this.expectedProcesses && this.expectedProcesses > 0) {
            this.allProcessesReady = true;
            console.log(`[ProcessManager] 🎯 All ${this.expectedProcesses} processes are ready!`);
            setTimeout(() => {
                this.emit('allProcessesReady');
            }, 1000);
        }
    }

    async restartProcess(processInfo) {
        const { type, processingWorkers, queueName, sqliteIndex, dbPath } = processInfo;
        
        console.log(`[ProcessManager] 🔄 Restarting process ${processInfo.pid} (${type})...`);
        this.processes.delete(processInfo.pid);
        this.expectedProcesses--;
        this.readyProcesses--;
        this.allProcessesReady = false;
        
        await this._sleep(this.restartDelay);
        
        const newProcess = await this.createProcess({
            type,
            processingWorkers,
            queueName,
            sqliteIndex,
            dbPath
        });
        
        console.log(`[ProcessManager] ✅ Process ${processInfo.pid} restarted as ${newProcess.pid}`);
        return newProcess;
    }

    async waitForProcess(type, timeout = 30000) {
        const startTime = Date.now();
        while (Date.now() - startTime < timeout) {
            for (const [pid, info] of this.processes) {
                if (info.type === type && info.status === 'running' && info.isReady) {
                    return info;
                }
            }
            await this._sleep(500);
        }
        return null;
    }

    getProcess(type) {
        for (const [pid, info] of this.processes) {
            if (info.type === type && info.status === 'running' && info.isReady) {
                return info;
            }
        }
        return null;
    }

    getProcessByPid(pid) {
        return this.processes.get(pid) || null;
    }

    getAllProcesses() {
        return Array.from(this.processes.values());
    }

    getProcessCount() {
        return this.processes.size;
    }

    getReadyCount() {
        return this.readyProcesses;
    }

    isAllReady() {
        return this.allProcessesReady;
    }

    sendMessage(pid, message) {
        const process = this.processes.get(pid);
        if (process && process.child) {
            try {
                process.child.send(message);
                return true;
            } catch (error) {
                console.error(`[ProcessManager] Failed to send message to ${pid}:`, error.message);
                return false;
            }
        }
        return false;
    }

    async shutdown() {
        console.log('[ProcessManager] 🛑 Shutting down all processes...');
        this.isRunning = false;
        
        const promises = [];
        for (const [pid, info] of this.processes) {
            promises.push(new Promise((resolve) => {
                try {
                    info.child.send({ type: 'SHUTDOWN' });
                } catch (err) {}
                info.child.on('exit', resolve);
                setTimeout(resolve, 3000);
            }));
        }
        
        await Promise.all(promises);
        this.processes.clear();
        this.expectedProcesses = 0;
        this.readyProcesses = 0;
        this.allProcessesReady = false;
        console.log('[ProcessManager] ✅ All processes shut down');
        this.emit('shutdown');
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = ProcessManager;