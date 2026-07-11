// index.js
const Application = require('./main');
const eventConfig = require('./event.json');

// ✅ Create application instance
const app = new Application({
    heartbeatTimeout: 15000,
    restartDelay: 2000,
    workers : {
        analyzer : 2,
        browser  : 1,
        exporter : 1,
        job_submitter : 2
    },
    submitInterval: 3000
});

// ✅ Export handler for Lambda
module.exports.handler = async (event, context) => {
    console.log('📋 Lambda invoked');
    
    try {
        const events = event.events || eventConfig;
        await app.start(events);
        
        // Wait for all processes to be ready
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

// ✅ Local run with async/await
if (require.main === module) {
    (async () => {
        console.log('🏠 Running in local mode...');
        
        try {
            // ✅ Start the application
            await app.start(eventConfig);
            console.log('[index] App started, checking if ready...');
            
            // ✅ If already ready, start job submitter immediately
            if (app.isReady) {
                console.log('[index] App is already ready, starting job submitter...');
                await app.startJobSubmitter();
                return;
            }
            
            // ✅ Wait for all processes to be ready
            console.log('[index] Waiting for allProcessesReady...');
            
            await new Promise((resolve) => {
                app.once('allProcessesReady', () => {
                    console.log('[index] allProcessesReady received!');
                    resolve();
                });
                
                // ✅ FALLBACK: If event doesn't fire within 15 seconds, force it
                setTimeout(() => {
                    if (!app.jobSubmitterStarted) {
                        console.log('[index] ⚠️ allProcessesReady timeout, forcing start...');
                        resolve();
                    }
                }, 15000);
            });
            
            // ✅ Start job submitter
            await app.startJobSubmitter();
            
            console.log('[index] ✅ Job submitter started successfully');
            
        } catch (error) {
            console.error('❌ Application failed:', error);
            process.exit(1);
        }
    })();

    // Graceful shutdown
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
}