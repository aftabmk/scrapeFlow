// test.js
const { fork } = require('child_process');
const path = require('path');
const Orchestrator = require('./parent/orchestrator');
const eventConfig = require('./event.json');

// ✅ Configuration
const config = {
    heartbeatTimeout: 15000,
    restartDelay: 2000,
    analyzerWorkers: 2,
    browserWorkers: 2,
    exporterWorkers: 1,
    submitInterval: 3000
};

class TestHarness {
    constructor() {
        this.orchestrator = null;
        this.sqliteServer = null;
        this.processes = {
            analyzer: null,
            browser: null,
            exporter: null,
            'job-submitter': null
        };
        this.isRunning = false;
        this.step = 0;
    }

    async start() {
        console.log('🧪 Starting Test Harness...\n');
        
        // Step 1: Create Orchestrator
        this.orchestrator = new Orchestrator({
            heartbeatTimeout: config.heartbeatTimeout,
            restartDelay: config.restartDelay
        });

        // Step 3: Start ONLY Job Submitter
        console.log('📦 Step 2: Starting Job Submitter...');
        await this.orchestrator.createProcess({
            type: 'job-submitter',
            processingWorkers: 5,
            queueName: 'job_submitter_queue'
        });
        console.log('✅ Job Submitter started\n');

        console.log('📊 Waiting 5 seconds for job-submitter to stabilize...');
        await this._sleep(5000);

        // Step 4: Display menu
        await this._showMenu();
    }

    async _showMenu() {
        console.log('\n' + '='.repeat(50));
        console.log('📋 TEST MENU');
        console.log('='.repeat(50));
        console.log('  1️⃣  Start Analyzer');
        console.log('  2️⃣  Start Browser');
        console.log('  3️⃣  Start Exporter');
        console.log('  4️⃣  Start ALL remaining processes');
        console.log('  5️⃣  Submit jobs (requires Analyzer running)');
        console.log('  6️⃣  Show process status');
        console.log('  7️⃣  Shutdown all');
        console.log('  8️⃣  Exit');
        console.log('='.repeat(50));

        // Read user input
        const readline = require('readline').createInterface({
            input: process.stdin,
            output: process.stdout
        });

        const ask = (question) => new Promise((resolve) => {
            readline.question(question, (answer) => {
                resolve(answer.trim());
                readline.close();
            });
        });

        const answer = await ask('\n👉 Enter your choice: ');

        switch (answer) {
            case '1':
                await this._startAnalyzer();
                break;
            case '2':
                await this._startBrowser();
                break;
            case '3':
                await this._startExporter();
                break;
            case '4':
                await this._startAllRemaining();
                break;
            case '5':
                await this._submitJobs();
                break;
            case '6':
                await this._showStatus();
                break;
            case '7':
                await this._shutdown();
                return;
            case '8':
                console.log('👋 Exiting...');
                process.exit(0);
                return;
            default:
                console.log('❌ Invalid choice. Try again.');
        }

        // Show menu again
        await this._showMenu();
    }

    async _startAnalyzer() {
        if (this.processes.analyzer) {
            console.log('⚠️ Analyzer already running');
            return;
        }
        console.log('📦 Starting Analyzer...');
        await this.orchestrator.createProcess({
            type: 'analyzer',
            processingWorkers: config.analyzerWorkers,
            queueName: 'analyzer_queue'
        });
        this.processes.analyzer = true;
        console.log('✅ Analyzer started\n');
    }

    async _startBrowser() {
        if (this.processes.browser) {
            console.log('⚠️ Browser already running');
            return;
        }
        console.log('📦 Starting Browser...');
        await this.orchestrator.createProcess({
            type: 'browser',
            processingWorkers: config.browserWorkers,
            queueName: 'browser_queue'
        });
        this.processes.browser = true;
        console.log('✅ Browser started\n');
    }

    async _startExporter() {
        if (this.processes.exporter) {
            console.log('⚠️ Exporter already running');
            return;
        }
        console.log('📦 Starting Exporter...');
        await this.orchestrator.createProcess({
            type: 'exporter',
            processingWorkers: config.exporterWorkers,
            queueName: 'export_queue'
        });
        this.processes.exporter = true;
        console.log('✅ Exporter started\n');
    }

    async _startAllRemaining() {
        console.log('📦 Starting ALL remaining processes...');
        await this._startAnalyzer();
        await this._startBrowser();
        await this._startExporter();
        console.log('✅ All processes started!\n');
    }

    async _submitJobs() {
        // Check if analyzer is running
        if (!this.processes.analyzer) {
            console.log('❌ Analyzer is not running. Please start it first.');
            return;
        }

        console.log('📤 Submitting jobs...');
        console.log(`📋 Events: ${eventConfig.length}\n`);

        // ✅ Start job submitter via orchestrator
        await this.orchestrator.startJobSubmitter({
            events: eventConfig,
            maxJobs: eventConfig.length,
            submitInterval: config.submitInterval
        });

        console.log('\n✅ Jobs submitted! Check the logs for processing.\n');
    }

    async _showStatus() {
        console.log('\n📊 Process Status:');
        console.log('='.repeat(40));
        console.log(`  SQLite Server:  ${this.orchestrator.sqliteServer ? '✅ Running' : '❌ Stopped'}`);
        console.log(`  Job Submitter:  ${this.processes['job-submitter'] !== null ? '✅ Running' : '❌ Stopped'}`);
        console.log(`  Analyzer:       ${this.processes.analyzer ? '✅ Running' : '⏳ Not started'}`);
        console.log(`  Browser:        ${this.processes.browser ? '✅ Running' : '⏳ Not started'}`);
        console.log(`  Exporter:       ${this.processes.exporter ? '✅ Running' : '⏳ Not started'}`);
        console.log('='.repeat(40) + '\n');
    }

    async _shutdown() {
        console.log('🛑 Shutting down all processes...');
        await this.orchestrator.shutdown();
        console.log('✅ Shutdown complete');
        process.exit(0);
    }

    async _sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// ============================================================
// Run the test harness
// ============================================================

const test = new TestHarness();

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n\n🛑 Received SIGINT');
    await test._shutdown();
});

process.on('SIGTERM', async () => {
    console.log('\n\n🛑 Received SIGTERM');
    await test._shutdown();
});

// Start test
test.start().catch((error) => {
    console.error('❌ Test failed:', error);
    process.exit(1);
});