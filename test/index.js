// index.js
const Orchestrator = require('./core/orchestrator');
const config = require('./config');

// Load events
let events = [];
try {
  events = require('./event.json');
  console.log(`📋 Loaded ${events.length} events from event.json`);
} catch (err) {
  console.warn('⚠️ event.json not found, using empty config');
}

// Create application
const app = new Orchestrator(config);

// Export handler for Lambda
module.exports.handler = async (event, context) => {
  console.log('📋 Lambda invoked');
  
  try {
    const events = event.events || require('./event.json');
    await app.start(events);
    
    await new Promise((resolve) => {
      app.once('pipeline.completed', resolve);
    });
    
    const status = app.getStatus();
    
    return {
      statusCode: 200,
      body: JSON.stringify({
        success: true,
        status,
        totalEvents: events.length,
      }),
    };
  } catch (error) {
    console.error('❌ Lambda error:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        success: false,
        error: error.message,
      }),
    };
  }
};

// Local run
if (require.main === module) {
  (async () => {
    console.log('🏠 Running in local mode...');
    console.log(`📊 PID: ${process.pid}`);
    console.log(`📊 Node: ${process.version}`);
    
    try {
      await app.start(events);
      
      console.log('✅ Application ready');
      console.log('📊 Press Ctrl+C to stop');
      
      const status = app.getStatus();
      console.log('\n📊 Status:');
      console.log(`  System: ${status.system.status}`);
      console.log(`  Health: ${status.health.status}`);
      console.log(`  Mode: ${status.memoryMode ? 'Memory' : 'Durable'}`);
      console.log(`  SQLite: ${status.sqliteReady ? 'Ready' : 'Starting'}`);
      console.log(`  Puppeteer: ${status.puppeteerReady ? 'Ready' : 'Starting'}`);
      console.log(`  Workers: ${status.workers.total || 0}`);
      
      if (events.length > 0) {
        console.log(`  Events: ${events.length}`);
        console.log(`  Pipeline: ${status.pipeline.status}`);
      }
      
      console.log('\n📊 Watching for events...');
      
    } catch (error) {
      console.error('❌ Application failed:', error);
      process.exit(1);
    }
  })();

  const gracefulShutdown = async (signal) => {
    console.log(`\n📊 Received ${signal}`);
    console.log('[Index] Starting graceful shutdown...');
    
    let forceExitTimer = null;
    let shutdownCompleted = false;
    
    try {
      forceExitTimer = setTimeout(() => {
        if (!shutdownCompleted) {
          console.error('[Index] ⚠️ Shutdown timeout, forcing exit...');
          process.exit(0);
        }
      }, 10000);
      
      if (!app._shuttingDown) {
        await app.shutdown();
      } else {
        console.log('[Index] Orchestrator already shutting down, waiting...');
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      shutdownCompleted = true;
      clearTimeout(forceExitTimer);
      
      console.log('[Index] Shutdown complete');
      process.exit(0);
      
    } catch (error) {
      console.error('[Index] Shutdown error:', error);
      clearTimeout(forceExitTimer);
      process.exit(1);
    }
  };

  process.on('SIGINT', () => gracefulShutdown('SIGINT'));
  process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));

  process.on('uncaughtException', (error) => {
    console.error('❌ Uncaught exception:', error);
    gracefulShutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason) => {
    console.error('❌ Unhandled rejection:', reason);
    gracefulShutdown('unhandledRejection');
  });
}

module.exports = app;