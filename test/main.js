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

    // === Generate unique SQLite index for each queue ===

    _getSqliteIndex() {
        this.sqliteIndex++;
        return this.sqliteIndex;
    }

    // === Start Application ===

    async start(eventConfig = []) {
        const events = eventConfig.length > 0 ? eventConfig : this.eventConfig;
        this.eventConfig = events;
        
        console.log('🚀 Starting Application...');
        console.log(`📋 Loaded ${events.length} events from config`);

        try {
            // 1. Create Orchestrator
            this.orchestrator = new Orchestrator({
                heartbeatTimeout: this.options.heartbeatTimeout || 15000,
                restartDelay: this.options.restartDelay || 2000
            });

            this._setupEventListeners();

            // 2. Start SQLite Server FIRST
            console.log('📦 Starting SQLite Server...');
            await this.orchestrator.startSQLiteServer({
                dbPath: this.options.dbPath || './data/queue.db',
                readWorkers: this.options.readWorkers || 3,
                writeWorkers: this.options.writeWorkers || 1
            });

            console.log('📦 Creating child processes...');

            // 3. Create Analyzer Process
            const analyzerIndex = this._getSqliteIndex();
            await this.orchestrator.createProcess({
                type: 'analyzer',
                processingWorkers: this.options.analyzerWorkers || 2,
                queueName: 'analyzer_queue',
                sqliteIndex: analyzerIndex,
                dbPath: this.options.dbPath || './data/queue.db'
            });

            // 4. Create Browser Process
            const browserIndex = this._getSqliteIndex();
            await this.orchestrator.createProcess({
                type: 'browser',
                processingWorkers: this.options.browserWorkers || 2,
                queueName: 'browser_queue',
                sqliteIndex: browserIndex,
                dbPath: this.options.dbPath || './data/queue.db'
            });

            // 5. Create Exporter Process
            const exporterIndex = this._getSqliteIndex();
            await this.orchestrator.createProcess({
                type: 'exporter',
                processingWorkers: this.options.exporterWorkers || 1,
                queueName: 'export_queue',
                sqliteIndex: exporterIndex,
                dbPath: this.options.dbPath || './data/queue.db'
            });

            // 6. Create Job-Submitter Process
            const submitterIndex = this._getSqliteIndex();
            await this.orchestrator.createProcess({
                type: 'job-submitter',
                processingWorkers: this.options.submitterWorkers || 5,
                queueName: 'job_submitter_queue',
                sqliteIndex: submitterIndex,
                dbPath: this.options.dbPath || './data/queue.db'
            });

            console.log('✅ All processes created!');

            // 7. Forward allProcessesReady from orchestrator
            this.orchestrator.once('allProcessesReady', () => {
                console.log('\n🎯 All processes ready! Forwarding to app...');
                this.isReady = true;
                this.emit('allProcessesReady');
            });

            // 8. Check if already ready (edge case)
            if (this.orchestrator.allProcessesReady) {
                console.log('\n🎯 All processes already ready! Forwarding to app...');
                this.isReady = true;
                this.emit('allProcessesReady');
            }

            // 9. FALLBACK: If allProcessesReady doesn't fire within 10 seconds, force it
            setTimeout(() => {
                if (!this.isReady) {
                    console.log('\n⚠️ allProcessesReady timeout, forcing ready state...');
                    this.isReady = true;
                    this.emit('allProcessesReady');
                }
            }, 10000);

            console.log('🎯 System ready!');
            console.log(`📊 SQLite Indexes: A${analyzerIndex}, B${browserIndex}, E${exporterIndex}, S${submitterIndex}`);
            console.log('📊 Press Ctrl+C to stop');
            
            return this;

        } catch (error) {
            console.error('❌ Failed to start application:', error);
            throw error;
        }
    }

    // === Event Listeners ===

    _setupEventListeners() {
        // Process ready event
        this.orchestrator.on('processReady', ({ pid, type, processingWorkers }) => {
            console.log(`✅ Process ${pid} (${type}) ready with ${processingWorkers} workers`);
        });

        // Process exit event
        this.orchestrator.on('processExit', ({ pid, type, code }) => {
            if (code !== 0) {
                console.log(`⚠️ Process ${pid} (${type}) exited with code ${code}`);
            }
        });

        // Job fully complete event
        this.orchestrator.on('jobFullyComplete', ({ jobId }) => {
            console.log(`🎉 Job ${jobId} fully completed through all stages!`);
        });

        // Submitter started event
        this.orchestrator.on('submitterStarted', ({ maxJobs, submitInterval }) => {
            console.log(`📤 Job submitter started: ${maxJobs} jobs, interval: ${submitInterval}ms`);
        });

        // Submitter complete event
        this.orchestrator.on('submitterComplete', ({ totalJobs }) => {
            console.log(`✅ All ${totalJobs} events processed by job-submitter!`);
        });

        // Job submitted event
        this.orchestrator.on('jobSubmitted', ({ jobNumber, totalJobs, jobId, eventData }) => {
            if (eventData) {
                console.log(`📤 Event ${jobNumber}/${totalJobs}: ${eventData.EXCHANGE} - ${eventData.CONTRACT} (${jobId})`);
            } else {
                console.log(`📤 Event ${jobNumber}/${totalJobs} submitted: ${jobId}`);
            }
        });

        // Job submission error event
        this.orchestrator.on('jobSubmissionError', ({ jobNumber, error }) => {
            console.error(`❌ Event ${jobNumber} submission failed:`, error);
        });

        // Heartbeat timeout event
        this.orchestrator.on('heartbeatTimeout', ({ pid }) => {
            console.log(`⏰ Heartbeat timeout for process ${pid}`);
        });
    }

    // === Start Job Submitter ===

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

    // === Shutdown ===

    async stop() {
        console.log('\n🛑 Shutting down...');
        if (this.orchestrator) {
            await this.orchestrator.shutdown();
        }
        console.log('✅ Shutdown complete');
        process.exit(0);
    }

    // === Getters ===

    getOrchestrator() {
        return this.orchestrator;
    }

    async getProcessStats() {
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