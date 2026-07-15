// test.js - Performance test file
const { performance } = require('perf_hooks');
const path = require('path');
const fs = require('fs');

// ✅ Import performance monitor
const perf = require('./performance/performance-monitor');

// ✅ Import application
const Application = require('./main');
const eventConfig = require('./event.json');

// ============================================================
// TEST CONFIGURATION
// ============================================================

const TEST_CONFIG = {
    iterations: 1,
    warmup: false,
    profile: process.env.PROFILE === 'true' || true,
    outputDir: './performance/metrics',
    testName: 'application-startup'
};

// ============================================================
// TEST HELPER FUNCTIONS
// ============================================================

function log(message, data = null) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${message}`);
    if (data) {
        console.log(JSON.stringify(data, null, 2));
    }
}

function ensureOutputDir() {
    if (!fs.existsSync(TEST_CONFIG.outputDir)) {
        fs.mkdirSync(TEST_CONFIG.outputDir, { recursive: true });
    }
}

function writeTestResult(result) {
    ensureOutputDir();
    const filename = `${TEST_CONFIG.testName}-${Date.now()}.json`;
    const filepath = path.join(TEST_CONFIG.outputDir, filename);
    fs.writeFileSync(filepath, JSON.stringify(result, null, 2));
    log(`📊 Test result written to: ${filepath}`);
}

// ============================================================
// MAIN TEST FUNCTION
// ============================================================

async function runPerformanceTest() {
    log('🚀 Starting Performance Test');
    log(`📊 Profile enabled: ${perf.enabled}`);
    log(`📊 Test Name: ${TEST_CONFIG.testName}`);
    log(`📊 Iterations: ${TEST_CONFIG.iterations}`);
    log('');

    const results = {
        testName: TEST_CONFIG.testName,
        timestamp: new Date().toISOString(),
        nodeVersion: process.version,
        platform: process.platform,
        iterations: [],
        summary: {
            totalTime: 0,
            avgTime: 0,
            minTime: Infinity,
            maxTime: 0,
            functionsTracked: 0,
            totalCalls: 0
        }
    };

    for (let i = 0; i < TEST_CONFIG.iterations; i++) {
        log(`\n📊 Iteration ${i + 1}/${TEST_CONFIG.iterations}`);
        
        const iterationResult = await runSingleIteration(i);
        results.iterations.push(iterationResult);
        
        // Update summary
        results.summary.totalTime += iterationResult.totalTime;
        results.summary.minTime = Math.min(results.summary.minTime, iterationResult.totalTime);
        results.summary.maxTime = Math.max(results.summary.maxTime, iterationResult.totalTime);
    }

    // Calculate averages
    results.summary.avgTime = results.summary.totalTime / results.iterations.length;
    
    // Get final performance summary
    const perfSummary = perf.getSummary();
    results.summary.functionsTracked = perfSummary.totalFunctions;
    results.summary.totalCalls = perfSummary.totalCalls;

    log('\n📊 ===== TEST COMPLETE =====');
    log(`📊 Total Iterations: ${results.iterations.length}`);
    log(`📊 Total Time: ${(results.summary.totalTime / 1000).toFixed(2)}s`);
    log(`📊 Avg Time: ${(results.summary.avgTime / 1000).toFixed(2)}s`);
    log(`📊 Min Time: ${(results.summary.minTime / 1000).toFixed(2)}s`);
    log(`📊 Max Time: ${(results.summary.maxTime / 1000).toFixed(2)}s`);
    log(`📊 Functions Tracked: ${results.summary.functionsTracked}`);
    log(`📊 Total Calls: ${results.summary.totalCalls}`);
    
    // Write results
    writeTestResult(results);
    
    // Stop performance monitor
    perf.stop();
    
    log('\n✅ Test completed successfully!');
}

// ============================================================
// SINGLE ITERATION
// ============================================================

async function runSingleIteration(iteration) {
    const startTime = performance.now();
    const iterationId = perf.start(`Test.iteration.${iteration}`);

    try {
        // ✅ Create application instance
        const app = new Application({
            heartbeatTimeout: 15000,
            restartDelay: 2000,
            analyzerWorkers: 2,
            browserWorkers: 2,
            exporterWorkers: 1,
            submitterWorkers: 5,
            submitInterval: 3000,
            dbPath: './data/queue.db',
            readWorkers: 3,
            writeWorkers: 1,
            sqliteTimeout: 10000,
            queueNames: ['analyzer', 'browser', 'exporter', 'job-submitter']
        });

        // ✅ Track application start
        const appStartId = perf.start('Application.start');

        // ✅ Start application
        await perf.time('Application.start.full', async () => {
            await app.start(eventConfig);
        });

        // ✅ Wait for ready
        log(`⏳ Waiting for allProcessesReady...`);
        await new Promise((resolve) => {
            if (app.isReady) {
                resolve();
            } else {
                app.once('allProcessesReady', resolve);
            }
        });

        // ✅ Start job submitter
        await perf.time('JobSubmitter.start', async () => {
            await app.startJobSubmitter();
        });

        // ✅ Wait a bit for processing
        await new Promise(resolve => setTimeout(resolve, 2000));

        // ✅ Shutdown
        await app.stop();

        perf.end(appStartId);

    } catch (error) {
        log(`❌ Iteration ${iteration} failed:`, error.message);
        perf.end(iterationId, { error: error.message });
        throw error;
    }

    const totalTime = performance.now() - startTime;
    perf.end(iterationId);

    return {
        iteration,
        totalTime,
        success: true,
        timestamp: new Date().toISOString()
    };
}

// ============================================================
// QUICK TEST (Minimal)
// ============================================================

async function runQuickTest() {
    log('🚀 Running Quick Performance Test');
    log(`📊 Profile enabled: ${perf.enabled}`);
    log('');

    const startTime = performance.now();

    try {
        const app = new Application({
            heartbeatTimeout: 15000,
            restartDelay: 2000,
            analyzerWorkers: 2,
            browserWorkers: 2,
            exporterWorkers: 1,
            submitterWorkers: 5,
            submitInterval: 3000,
            dbPath: './data/queue.db',
            readWorkers: 3,
            writeWorkers: 1,
            sqliteTimeout: 10000,
            queueNames: ['analyzer', 'browser', 'exporter', 'job-submitter']
        });

        const appStartId = perf.start('Application.start.quick');

        await app.start(eventConfig);

        await new Promise((resolve) => {
            if (app.isReady) {
                resolve();
            } else {
                app.once('allProcessesReady', resolve);
            }
        });

        await app.startJobSubmitter();

        await new Promise(resolve => setTimeout(resolve, 1000));

        await app.stop();

        perf.end(appStartId);

        const totalTime = performance.now() - startTime;
        log(`\n✅ Quick test completed in ${(totalTime / 1000).toFixed(2)}s`);

        perf.stop();

    } catch (error) {
        log(`❌ Quick test failed:`, error.message);
        perf.stop();
        process.exit(1);
    }
}

// ============================================================
// RUN TESTS
// ============================================================

if (require.main === module) {
    const args = process.argv.slice(2);
    const isQuick = args.includes('--quick') || args.includes('-q');

    if (isQuick) {
        runQuickTest();
    } else {
        runPerformanceTest();
    }
}

// ============================================================
// EXPORTS
// ============================================================

module.exports = {
    runPerformanceTest,
    runQuickTest,
    perf
};