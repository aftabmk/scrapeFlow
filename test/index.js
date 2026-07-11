// index.js
const Application = require('./main');
const config = require('./config');

// ✅ Create application instance
const app = new Application(config);

// ✅ Lambda Handler
const handler = async (event, context) => {
  console.log('📋 Lambda invoked');
  
  try {
    // Override events from Lambda if provided
    if (event.events) {
      config.events = event.events;
    }
    
    await app.start();
    
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        success: true,
        message: 'Application started',
        totalEvents: config.events.length,
        timestamp: new Date().toISOString()
      })
    };
  } catch (error) {
    console.error('❌ Lambda error:', error);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        success: false,
        error: error.message,
        timestamp: new Date().toISOString()
      })
    };
  }
};

// ✅ Export everything
module.exports = {
  app,
  handler,
  config,
  Application
};

// ✅ Local run (auto-detects if this is the main module)
if (require.main === module) {
  // Start with config
  app.start().catch((error) => {
    console.error('❌ Application failed:', error);
    process.exit(1);
  });

  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('\nReceived SIGINT');
    await app.stop();
    process.exit(0);
  });

  process.on('SIGTERM', async () => {
    console.log('\nReceived SIGTERM');
    await app.stop();
    process.exit(0);
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