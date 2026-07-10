// parent/orchestrator.js
const { fork } = require('child_process');
const path = require('path');
const { EventEmitter } = require('events');

class Orchestrator extends EventEmitter {
	constructor(options = {}) {
		super();
		this.processes = new Map();
		this.sqliteServer = null;
		this.heartbeatTimeout = options.heartbeatTimeout || 10000;
		this.restartDelay = options.restartDelay || 1000;
		this.isRunning = true;

		this._startHeartbeatMonitor();
	}

	async startSQLiteServer(options = {}) {
		console.log('[Orchestrator] Starting SQLite Server...');

		return new Promise((resolve, reject) => {
			const serverPath = path.join(__dirname, '../sqlite-server/index.js');

			// Fork the SQLite Server process
			this.sqliteServer = fork(serverPath, [], {
				stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
				execArgv: ['--experimental-sqlite'],
				env: {
					...process.env,
					DB_PATH: options.dbPath || './data/queue.db',
					READ_WORKERS: options.readWorkers || 3
				}
			});

			console.log('[Orchestrator] SQLite Server PID:', this.sqliteServer.pid);

			let resolved = false;

			// ✅ Listen for messages from SQLite Server
			this.sqliteServer.on('message', (msg) => {
				console.log('[Orchestrator] Received message:', msg?.type);

				if (msg.type === 'SQLITE_READY') {
					if (!resolved) {
						resolved = true;
						console.log('[Orchestrator] ✅ SQLite Server ready');
						resolve(msg);
					}
				}
			});

			// Handle errors
			this.sqliteServer.on('error', (err) => {
				console.error('[Orchestrator] SQLite Server error:', err);
				if (!resolved) {
					resolved = true;
					reject(err);
				}
			});

			// Handle exit
			this.sqliteServer.on('exit', (code, signal) => {
				console.log(`[Orchestrator] SQLite Server exited with code ${code}, signal ${signal}`);

				if (this.isRunning) {
					if (!resolved) {
						resolved = true;
						reject(new Error(`SQLite Server exited with code ${code}`));
					} else {
						// Restart if it was running
						console.log('[Orchestrator] Restarting SQLite Server...');
						setTimeout(() => this.startSQLiteServer(options), 2000);
					}
				}
			});

			// ✅ Timeout for startup
			const timeout = setTimeout(() => {
				if (!resolved) {
					resolved = true;
					console.error('[Orchestrator] SQLite Server startup timeout');
					this.sqliteServer.kill();
					reject(new Error('SQLite Server startup timeout (30s)'));
				}
			}, 30000);

			// Cleanup timeout on resolve/reject
			const cleanup = () => clearTimeout(timeout);
			const origResolve = resolve;
			resolve = (value) => { cleanup(); origResolve(value); };
			const origReject = reject;
			reject = (value) => { cleanup(); origReject(value); };
		});
	}

	createProcess(options = {}) {
		const {
			type = 'browser',
			processingWorkers = 2,
			commWorkers = 1,
			queueName = `${type}_queue`,
			args = []
		} = options;

		const scriptMap = {
			browser: './child/browser.js',
			analyzer: './child/analyzer.js',
			exporter: './child/exporter.js'
		};

		const scriptPath = scriptMap[type] || scriptMap.browser;
		const fullPath = path.join(__dirname, '..', scriptPath);

		const child = fork(fullPath, [
			`--processing-workers=${processingWorkers}`,
			`--comm-workers=${commWorkers}`,
			`--queue-name=${queueName}`,
			`--process-type=${type}`,
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
			commWorkers,
			queueName,
			lastHeartbeat: Date.now(),
			status: 'starting',
			createdAt: Date.now(),
			restartCount: 0
		};

		child.on('message', (msg) => {
			this._handleChildMessage(processInfo, msg);
		});

		child.on('exit', (code, signal) => {
			this._handleChildExit(processInfo, code, signal);
		});

		child.on('error', (err) => {
			console.error(`Child ${child.pid} error:`, err);
			this._handleChildExit(processInfo, -1, 'error');
		});

		this.processes.set(child.pid, processInfo);

		return new Promise((resolve) => {
			const readyHandler = (msg) => {
				if (msg.type === 'ready' && msg.processType === type) {
					processInfo.status = 'running';
					this.emit('processReady', { pid: child.pid, type, ...msg });
					resolve(processInfo);
				}
			};

			child.on('message', readyHandler);

			setTimeout(() => {
				child.removeListener('message', readyHandler);
				if (processInfo.status === 'starting') {
					processInfo.status = 'timeout';
					this.emit('processTimeout', { pid: child.pid, type });
					resolve(processInfo);
				}
			}, 5000);
		});
	}

	_handleChildMessage(processInfo, message) {
		if (!message || !message.type) return;

		switch (message.type) {
			case 'HEARTBEAT':
				processInfo.lastHeartbeat = Date.now();
				this.emit('heartbeat', { pid: processInfo.pid, stats: message.stats });
				break;

			case 'JOB_QUEUED':
				this.emit('jobQueued', { pid: processInfo.pid, jobId: message.jobId });
				break;

			case 'JOB_ERROR':
				this.emit('jobError', { pid: processInfo.pid, jobId: message.jobId, error: message.error });
				break;

			case 'STATUS':
				this.emit('status', { pid: processInfo.pid, ...message });
				break;

			case 'SHUTDOWN_COMPLETE':
				processInfo.status = 'stopped';
				this.emit('shutdownComplete', { pid: processInfo.pid });
				break;

			default:
				this.emit('message', { pid: processInfo.pid, message });
		}
	}

	_handleChildExit(processInfo, code, signal) {
		processInfo.status = 'exited';
		processInfo.exitCode = code;
		processInfo.exitSignal = signal;

		this.emit('processExit', {
			pid: processInfo.pid,
			type: processInfo.type,
			code,
			signal
		});

		if (this.isRunning) {
			this._restartProcess(processInfo);
		}
	}

	async _restartProcess(processInfo) {
		const { type, processingWorkers, commWorkers, queueName } = processInfo;

		console.log(`Restarting process ${processInfo.pid} (${type})...`);

		this.processes.delete(processInfo.pid);

		await this._sleep(this.restartDelay);

		const newProcess = await this.createProcess({
			type,
			processingWorkers,
			commWorkers,
			queueName
		});

		console.log(`Process ${processInfo.pid} restarted as ${newProcess.pid}`);
	}

	_startHeartbeatMonitor() {
		setInterval(() => {
			const now = Date.now();

			for (const [pid, info] of this.processes) {
				if (info.status === 'running') {
					const elapsed = now - info.lastHeartbeat;

					if (elapsed > this.heartbeatTimeout) {
						console.warn(`Process ${pid} heartbeat timeout (${elapsed}ms)`);
						this.emit('heartbeatTimeout', { pid, info });

						if (this.isRunning) {
							this._restartProcess(info);
						}
					}
				}
			}
		}, 2000);
	}

	async submitJob(jobData) {
		const { type = 'browser', data } = jobData;

		let targetProcess = null;
		for (const [pid, info] of this.processes) {
			if (info.type === type && info.status === 'running') {
				targetProcess = info;
				break;
			}
		}

		if (!targetProcess) {
			throw new Error(`No running process of type: ${type}`);
		}

		const jobId = `job_${Date.now()}_${Math.random().toString(36).substr(2, 6)}`;

		return new Promise((resolve, reject) => {
			const timeout = setTimeout(() => {
				reject(new Error('Job submission timeout'));
			}, 10000);

			const listener = (msg) => {
				if (msg.type === 'JOB_QUEUED' && msg.jobId === jobId) {
					clearTimeout(timeout);
					targetProcess.child.removeListener('message', listener);
					resolve({ jobId, status: 'queued' });
				}
				if (msg.type === 'JOB_ERROR' && msg.jobId === jobId) {
					clearTimeout(timeout);
					targetProcess.child.removeListener('message', listener);
					reject(new Error(msg.error));
				}
			};

			targetProcess.child.on('message', listener);
			targetProcess.child.send({
				type: 'NEW_JOB',
				jobId,
				data
			});
		});
	}

	async getProcessStats() {
		const stats = {};
		for (const [pid, info] of this.processes) {
			stats[pid] = {
				type: info.type,
				status: info.status,
				processingWorkers: info.processingWorkers,
				queueName: info.queueName,
				lastHeartbeat: info.lastHeartbeat,
				uptime: Date.now() - info.createdAt,
				restartCount: info.restartCount
			};
		}
		return stats;
	}

	async shutdown() {
		this.isRunning = false;

		const promises = [];
		for (const [pid, info] of this.processes) {
			promises.push(new Promise((resolve) => {
				info.child.send({ type: 'SHUTDOWN' });
				info.child.on('exit', resolve);
				setTimeout(resolve, 5000);
			}));
		}

		await Promise.all(promises);
		this.processes.clear();

		if (this.sqliteServer) {
			this.sqliteServer.send({ type: 'SHUTDOWN' });
			await new Promise((resolve) => {
				this.sqliteServer.on('exit', resolve);
				setTimeout(resolve, 5000);
			});
		}

		this.emit('shutdown');
	}

	_sleep(ms) {
		return new Promise(resolve => setTimeout(resolve, ms));
	}
}

module.exports = Orchestrator;