// index.js
const Application = require('./main');
const eventConfig = require('./event.json');

// Create application instance
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

// Lambda handler
module.exports.handler = async (event, context) => {
    console.log('📋 Lambda invoked');
    
    try {
        const events = event.events || eventConfig;
        await app.start(events);
        
        await new Promise((resolve) => {
            if (app.isReady) {
                resolve();
            } else {
                app.once('allProcessesReady', resolve);
            }
        });
        
        await app.startJobSubmitter();
        
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

// Local run
if (require.main === module) {
    (async () => {
        console.log('🏠 Running in local mode...');
        console.log(`📊 PID: ${process.pid}`);
        console.log(`📊 Node version: ${process.version}`);

        try {
            await app.start(eventConfig);
            console.log('[index] App started, checking if ready...');

            if (app.isReady) {
                console.log('[index] App is already ready, starting job submitter...');
                await app.startJobSubmitter();
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

            await app.startJobSubmitter();
            console.log('[index] ✅ Job submitter started successfully');

        } catch (error) {
            console.error('❌ Application failed:', error);
            process.exit(1);
        }
    })();

    process.on('SIGINT', async () => {
        console.log('\nReceived SIGINT');
        await app.stop();
    });

    process.on('SIGTERM', async () => {
        console.log('\nReceived SIGTERM');
        await app.stop();
    });

    process.on('uncaughtException', (error) => {
        console.error('❌ Uncaught exception:', error);
        process.exit(1);
    });

    process.on('unhandledRejection', (reason) => {
        console.error('❌ Unhandled rejection:', reason);
        process.exit(1);
    });
} else {
    console.log('📦 Running in module mode - exported for external use');
}