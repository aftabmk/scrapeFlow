// index.js
const Application = require('./main');
const eventConfig = require('./event.json');
const perf = require('./utils/performance-monitor');

// ✅ Pass perf to application
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
    perf: perf
});

// ✅ Lambda handler
module.exports.handler = async (event, context) => {
    console.log('📋 Lambda invoked');
    
    try {
        const events = event.events || eventConfig;
        await perf.time('Lambda.handler', async () => {
            await app.start(events);
        });
        
        return {
            statusCode: 200,
            body: JSON.stringify({
                success: true,
                message: 'Application started',
                totalEvents: events.length
            })
        };
    } catch (error) {
        console.error('❌ Lambda error:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                success: false,
                error: error.message
            })
        };
    }
};

module.exports.app = app;
module.exports.perf = perf;

// ✅ Local run
if (require.main === module) {
    (async () => {
        console.log('🏠 Running in local mode...');
        console.log(`📊 Performance monitoring: ${perf.enabled ? '✅ ENABLED' : '❌ DISABLED'}`);
        console.log(`📊 PID: ${process.pid}`);
        console.log(`📊 Node version: ${process.version}`);

        try {
            const appStartId = perf.start('Application.start.full');
            
            await perf.time('Application.start.full', async () => {
                await app.start(eventConfig);
            });

            console.log('[index] App started, checking if ready...');

            if (app.isReady) {
                console.log('[index] App is already ready, starting job submitter...');
                await perf.time('JobSubmitter.start', async () => {
                    await app.startJobSubmitter();
                });
                return;
            }

            console.log('[index] Waiting for allProcessesReady...');

            await new Promise((resolve) => {
                app.once('allProcessesReady', () => {
                    console.log('[index] allProcessesReady received!');
                    resolve();
                });

                setTimeout(() => {
                    if (!app.jobSubmitterStarted) {
                        console.log('[index] ⚠️ allProcessesReady timeout, forcing start...');
                        resolve();
                    }
                }, 15000);
            });

            await perf.time('JobSubmitter.start', async () => {
                await app.startJobSubmitter();
            });

            console.log('[index] ✅ Job submitter started successfully');

        } catch (error) {
            console.error('❌ Application failed:', error);
            perf.stop();
            process.exit(1);
        }

        perf.end(appStartId);
    })();

    // ✅ Graceful shutdown
    process.on('SIGINT', async () => {
        console.log('\nReceived SIGINT');
        perf.stop();
        await app.stop();
        process.exit(0);
    });

    process.on('SIGTERM', async () => {
        console.log('\nReceived SIGTERM');
        perf.stop();
        await app.stop();
        process.exit(0);
    });

    process.on('uncaughtException', (error) => {
        console.error('❌ Uncaught exception:', error);
        perf.stop();
        process.exit(1);
    });

    process.on('unhandledRejection', (reason) => {
        console.error('❌ Unhandled rejection:', reason);
        perf.stop();
        process.exit(1);
    });

    process.on('exit', () => {
        if (perf.enabled) {
            perf.writeReport();
        }
    });
} else {
    console.log('📦 Running in module mode - exported for external use');
}