// puppeteer-server/index.js
const PuppeteerServer = require('./server');

// Parse command line arguments
const args = process.argv.slice(2);
const options = {
    tabCount: 5,
    headless: true,
    devtools: false,
    timeout: 30000,
};

for (let i = 0; i < args.length; i++) {
    const arg = args[i];
    if (arg.startsWith('--tab-count=')) {
        options.tabCount = parseInt(arg.split('=')[1]) || 4;
    } else if (arg.startsWith('--headless=')) {
        options.headless = arg.split('=')[1] === 'true';
    } else if (arg.startsWith('--devtools=')) {
        options.devtools = arg.split('=')[1] === 'true';
    } else if (arg.startsWith('--timeout=')) {
        options.timeout = parseInt(arg.split('=')[1]) || 30000;
    }
}

if (require.main === module) {
    console.log('[PuppeteerServer] 🚀 Starting Puppeteer Server process...');
    console.log(`[PuppeteerServer] PID: ${process.pid}`);
    console.log(`[PuppeteerServer] Tab Count: ${options.tabCount}`);
    console.log(`[PuppeteerServer] Headless: ${options.headless}`);
    console.log(`[PuppeteerServer] DevTools: ${options.devtools}`);
    console.log(`[PuppeteerServer] Timeout: ${options.timeout}ms`);

    // Handle uncaught exceptions
    process.on('uncaughtException', (err) => {
        console.error('[PuppeteerServer] Uncaught exception:', err.message);
        console.error('[PuppeteerServer] Stack:', err.stack);
        process.exit(1);
    });

    process.on('unhandledRejection', (reason) => {
        console.error('[PuppeteerServer] Unhandled rejection:', reason);
        process.exit(1);
    });

    try {
        const server = new PuppeteerServer(options);

        server.on('ready', (info) => {
            console.log('[PuppeteerServer] ✅ Ready');
        });

        server.start().catch((err) => {
            console.error('[PuppeteerServer] Failed to start:', err.message);
            console.error('[PuppeteerServer] Stack:', err.stack);
            process.exit(1);
        });

        process.on('message', async (message) => {
            if (message && message.type === 'SHUTDOWN') {
                console.log('[PuppeteerServer] Received shutdown command');
                await server.shutdown();
            }
        });

        process.on('SIGINT', async () => {
            console.log('[PuppeteerServer] SIGINT received');
            await server.shutdown();
        });

        process.on('SIGTERM', async () => {
            console.log('[PuppeteerServer] SIGTERM received');
            await server.shutdown();
        });

    } catch (error) {
        console.error('[PuppeteerServer] Fatal error:', error.message);
        console.error('[PuppeteerServer] Stack:', error.stack);
        process.exit(1);
    }
}

module.exports = PuppeteerServer;