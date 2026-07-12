// parent/components/process-creator.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class ProcessCreator extends EventEmitter {
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
            serverReady = false,  // ✅ serverReady flag from parent
            args = []
        } = options;

        const scriptPath = this.scriptMap[type] || this.scriptMap.browser;
        const fullPath = path.join(__dirname, '../..', scriptPath);

        // ✅ Pass serverReady as command-line arg
        const child = fork(fullPath, [
            `--processing-workers=${processingWorkers}`,
            `--queue-name=${queueName}`,
            `--process-type=${type}`,
            `--sqlite-index=${sqliteIndex}`,
            `--db-path=${dbPath}`,
            `--server-ready=${serverReady}`,  // ✅ Pass serverReady flag
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
            serverReady,  // ✅ Store serverReady
            lastHeartbeat: Date.now(),
            status: 'starting',
            createdAt: Date.now(),
            restartCount: 0,
            isReady: false,
            isAlive: false
        };

        child.on('message', (msg) => {
            this.emit('message', processInfo, msg);
            
            // ✅ Handle ALIVE signal (normal startup, no recover)
            if (msg.type === 'ALIVE' && msg.processType === type) {
                processInfo.status = 'running';
                processInfo.isAlive = true;
                console.log(`[ProcessCreator] ✅ Process ${child.pid} (${type}) alive (ALIVE)`);
                this.emit('processReady', { pid: child.pid, type, ...msg });
            }
            
            // ✅ Handle READY signal (after recover)
            if (msg.type === 'READY' && msg.processType === type) {
                processInfo.isReady = true;
                console.log(`[ProcessCreator] ✅ Process ${child.pid} (${type}) ready (READY)`);
                this.emit('processReadyAfterRecover', { pid: child.pid, type, ...msg });
            }
        });

        child.on('exit', (code, signal) => {
            this.emit('exit', processInfo, code, signal);
        });

        child.on('error', (err) => {
            console.error(`[ProcessCreator] Child ${child.pid} error:`, err);
            this.emit('exit', processInfo, -1, 'error');
        });

        this.processes.set(child.pid, processInfo);
        this.expectedProcesses++;

        return new Promise((resolve) => {
            // ✅ Wait for either ALIVE or READY signal (whichever comes first)
            const handler = (msg) => {
                if (msg.type === 'ALIVE' && msg.processType === type) {
                    resolve(processInfo);
                    child.off('message', handler);
                }
                if (msg.type === 'READY' && msg.processType === type) {
                    resolve(processInfo);
                    child.off('message', handler);
                }
            };
            
            child.on('message', handler);
            
            // ✅ Timeout for startup
            setTimeout(() => {
                child.off('message', handler);
                if (processInfo.status === 'starting') {
                    processInfo.status = 'timeout';
                    console.warn(`[ProcessCreator] Process ${child.pid} (${type}) startup timeout`);
                    this.emit('processTimeout', { pid: child.pid, type });
                    resolve(processInfo);
                }
            }, 15000);
        });
    }

    async restartProcess(processInfo) {
        const { type, processingWorkers, queueName, sqliteIndex, dbPath, serverReady } = processInfo;
        
        console.log(`[ProcessCreator] 🔄 Restarting process ${processInfo.pid} (${type})...`);
        this.processes.delete(processInfo.pid);
        
        // ✅ On restart, serverReady should be true (server is already running)
        await this._sleep(this.restartDelay);
        
        const newProcess = await this.createProcess({
            type,
            processingWorkers,
            queueName,
            sqliteIndex,
            dbPath,
            serverReady: true  // ✅ Server is already running
        });
        
        console.log(`[ProcessCreator] ✅ Process ${processInfo.pid} restarted as ${newProcess.pid}`);
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

    sendMessage(pid, message) {
        const process = this.processes.get(pid);
        if (process && process.child) {
            try {
                process.child.send(message);
                return true;
            } catch (error) {
                console.error(`[ProcessCreator] Failed to send message to ${pid}:`, error.message);
                return false;
            }
        }
        return false;
    }

    async shutdown() {
        console.log('[ProcessCreator] 🛑 Shutting down all processes...');
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
        console.log('[ProcessCreator] ✅ All processes shut down');
        this.emit('shutdown');
    }

    _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

module.exports = ProcessCreator;