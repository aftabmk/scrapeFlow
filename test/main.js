// main.js
const { EventEmitter } = require('events');
const Orchestrator = require('./parent/orchestrator');

class Application extends EventEmitter {
    constructor(options = {}) {
        super();
        this.orchestrator = null;
        this.jobSubmitterStarted = false;
        this.options = options;
        this.eventConfig = options.events || [];
        this.isReady = false;
        this.sqliteIndex = 0;
    }

    _getSqliteIndex() {
        this.sqliteIndex++;
        return this.sqliteIndex;
    }

    async start(eventConfig = []) {
        const events = eventConfig.length > 0 ? eventConfig : this.eventConfig;
        this.eventConfig = events;
        
        console.log('🚀 Starting Application...');
        console.log(`📋 Loaded ${events.length} events from config`);

        try {
            // 1. Create Orchestrator
            this.orchestrator = new Orchestrator({
                heartbeatTimeout: this.options.heartbeatTimeout || 15000,
                restartDelay: this.options.restartDelay || 2000,
                dbPath: this.options.dbPath || './data/queue.db',
                readWorkers: this.options.readWorkers || 3,
                writeWorkers: this.options.writeWorkers || 1,
                sqliteTimeout: this.options.sqliteTimeout || 10000,
                queueNames: this.options.queueNames || ['analyzer', 'browser', 'exporter', 'job-submitter']
            });

            this._setupEventListeners();

            // 2. Start SQLite Server (async)
            console.log('📦 Starting SQLite Server...');
            await this.orchestrator.startSQLiteServer();

            // 3. Define process configurations
            const processConfigs = [
                {
                    type: 'analyzer',
                    processingWorkers: this.options.analyzerWorkers || 2,
                    queueName: 'analyzer_queue',
                    sqliteIndex: this._getSqliteIndex(),
                    dbPath: this.options.dbPath || './data/queue.db'
                },
                {
                    type: 'browser',
                    processingWorkers: this.options.browserWorkers || 2,
                    queueName: 'browser_queue',
                    sqliteIndex: this._getSqliteIndex(),
                    dbPath: this.options.dbPath || './data/queue.db'
                },
                {
                    type: 'exporter',
                    processingWorkers: this.options.exporterWorkers || 1,
                    queueName: 'export_queue',
                    sqliteIndex: this._getSqliteIndex(),
                    dbPath: this.options.dbPath || './data/queue.db'
                },
                {
                    type: 'job-submitter',
                    processingWorkers: this.options.submitterWorkers || 5,
                    queueName: 'job_submitter_queue',
                    sqliteIndex: this._getSqliteIndex(),
                    dbPath: this.options.dbPath || './data/queue.db'
                }
            ];

            // 4. Spawn ALL children in parallel
            console.log('📦 Creating child processes (parallel)...');
            await this.orchestrator.createAllProcesses(processConfigs);

            console.log('✅ All processes created!');

            // 5. Wait for all processes to be ready
            this.orchestrator.once('allProcessesReady', () => {
                console.log('\n🎯 All processes ready! Forwarding to app...');
                this.isReady = true;
                this.emit('allProcessesReady');
            });

            if (this.orchestrator.allProcessesReady) {
                console.log('\n🎯 All processes already ready! Forwarding to app...');
                this.isReady = true;
                this.emit('allProcessesReady');
            }

            // 6. Fallback timeout
            setTimeout(() => {
                if (!this.isReady) {
                    console.log('\n⚠️ allProcessesReady timeout, forcing ready state...');
                    this.isReady = true;
                    this.emit('allProcessesReady');
                }
            }, 15000);

            console.log('🎯 System ready!');
            console.log('📊 Press Ctrl+C to stop');
            
            return this;

        } catch (error) {
            console.error('❌ Failed to start application:', error);
            throw error;
        }
    }

    _setupEventListeners() {
        this.orchestrator.on('processReady', ({ pid, type, processingWorkers }) => {
            console.log(`✅ Process ${pid} (${type}) alive with ${processingWorkers} workers`);
        });

        this.orchestrator.on('processReadyAfterRecover', ({ pid, type }) => {
            console.log(`✅ Process ${pid} (${type}) ready after recover`);
        });

        this.orchestrator.on('processExit', ({ pid, type, code }) => {
            if (code !== 0) {
                console.log(`⚠️ Process ${pid} (${type}) exited with code ${code}`);
            }
        });

        this.orchestrator.on('jobFullyComplete', ({ jobId }) => {
            console.log(`🎉 Job ${jobId} fully completed through all stages!`);
        });

        this.orchestrator.on('submitterStarted', ({ maxJobs, submitInterval }) => {
            console.log(`📤 Job submitter started: ${maxJobs} jobs, interval: ${submitInterval}ms`);
        });

        this.orchestrator.on('submitterComplete', ({ totalJobs }) => {
            console.log(`✅ All ${totalJobs} events processed by job-submitter!`);
        });

        this.orchestrator.on('jobSubmitted', ({ jobNumber, totalJobs, jobId, eventData }) => {
            if (eventData) {
                console.log(`📤 Event ${jobNumber}/${totalJobs}: ${eventData.EXCHANGE} - ${eventData.CONTRACT} (${jobId})`);
            } else {
                console.log(`📤 Event ${jobNumber}/${totalJobs} submitted: ${jobId}`);
            }
        });

        this.orchestrator.on('jobSubmissionError', ({ jobNumber, error }) => {
            console.error(`❌ Event ${jobNumber} submission failed:`, error);
        });

        this.orchestrator.on('heartbeatTimeout', ({ pid }) => {
            console.log(`⏰ Heartbeat timeout for process ${pid}`);
        });
    }

    async startJobSubmitter() {
        if (this.jobSubmitterStarted) {
            console.log('[Application] Job submitter already started');
            return;
        }
        this.jobSubmitterStarted = true;
        
        if (this.orchestrator) {
            const events = this.eventConfig || [];
            console.log(`[Application] 🚀 Starting job submitter with ${events.length} events`);
            
            if (events.length > 0) {
                console.log(`[Application] 📋 Events:`, events.map(e => `${e.EXCHANGE}-${e.CONTRACT}`).join(', '));
            }
            
            await this.orchestrator.startJobSubmitter({
                events: events,
                maxJobs: events.length,
                submitInterval: this.options.submitInterval || 3000
            });
        } else {
            console.error('[Application] Orchestrator not available');
        }
    }

    async stop() {
        console.log('\n🛑 Shutting down...');
        if (this.orchestrator) {
            await this.orchestrator.shutdown();
        }
        console.log('✅ Shutdown complete');
        process.exit(0);
    }

    getOrchestrator() {
        return this.orchestrator;
    }

    getProcessStats() {
        if (this.orchestrator) {
            return this.orchestrator.getProcessStats();
        }
        return null;
    }

    async submitJob(jobData) {
        if (this.orchestrator) {
            return this.orchestrator.submitJob(jobData);
        }
        throw new Error('Orchestrator not initialized');
    }
}

module.exports = Application;