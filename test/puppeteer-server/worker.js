// puppeteer-server/worker.js
const { parentPort, workerData } = require('worker_threads');
const puppeteer = require('puppeteer');

class PuppeteerWorker {
    constructor() {
        this.id = workerData.id || `puppeteer_${Date.now()}`;
        this.type = 'puppeteer';
        this.isRunning = true;
        
        this.tabCount = workerData.tabCount || parseInt(process.env.PUPPETEER_TABS) || 5;
        this.headless = workerData.headless !== false;
        this.devtools = workerData.devtools || false;
        this.timeout = workerData.timeout || 30000;
        
        this.browser = null;
        this.tabs = [];
        this.tabPool = [];
        this.isReady = false;
        
        console.log(`[PuppeteerWorker ${this.id}] Config: ${this.tabCount} tabs`);
        
        this.sendReady();
        this.start();
    }

    sendReady() {
        if (parentPort) {
            parentPort.postMessage({
                type: 'worker.ready',
                workerId: this.id,
                workerType: this.type,
                timestamp: Date.now()
            });
        }
    }

    async start() {
        if (parentPort) {
            parentPort.on('message', async (message) => {
                await this.handleMessage(message);
            });
        }
        await this.initBrowser();
    }

    async initBrowser() {
        try {
            console.log(`[PuppeteerWorker ${this.id}] Launching browser...`);
            
            this.browser = await puppeteer.launch({
                headless: this.headless,
                devtools: this.devtools,
                args: [
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--window-size=1920,1080',
                ],
                timeout: this.timeout,
            });
            
            console.log(`[PuppeteerWorker ${this.id}] ✅ Browser launched`);
            await this.createTabs();
            
            this.isReady = true;
            
            if (parentPort) {
                parentPort.postMessage({
                    type: 'PUPPETEER_READY',
                    workerId: this.id,
                    tabCount: this.tabCount,
                    timestamp: Date.now()
                });
            }
            
            console.log(`[PuppeteerWorker ${this.id}] ✅ Ready with ${this.tabCount} tabs`);
            
        } catch (error) {
            console.error(`[PuppeteerWorker ${this.id}] ❌ Start error:`, error.message);
            if (parentPort) {
                parentPort.postMessage({
                    type: 'PUPPETEER_ERROR',
                    error: error.message,
                    timestamp: Date.now()
                });
            }
        }
    }

    async createTabs() {
        console.log(`[PuppeteerWorker ${this.id}] Creating ${this.tabCount} tabs...`);
        
        for (let i = 0; i < this.tabCount; i++) {
            try {
                const page = await this.browser.newPage();
                page.setDefaultTimeout(this.timeout);
                page.setDefaultNavigationTimeout(this.timeout);
                await page.goto('about:blank', { waitUntil: 'load' });
                
                const tab = {
                    id: `tab_${i + 1}`,
                    page,
                    status: 'idle',
                    currentUrl: null,
                    lastUsed: Date.now(),
                    created: Date.now(),
                    stats: { scraped: 0, errors: 0, totalTime: 0 },
                };
                
                page.on('error', (error) => {
                    console.error(`[PuppeteerWorker] Tab ${tab.id} error:`, error.message);
                    tab.status = 'crashed';
                    this.recycleTab(tab);
                });
                
                page.on('pageerror', (error) => {
                    console.error(`[PuppeteerWorker] Tab ${tab.id} page error:`, error.message);
                });
                
                this.tabs.push(tab);
                this.tabPool.push(tab);
                console.log(`[PuppeteerWorker] Created tab: ${tab.id}`);
                
            } catch (error) {
                console.error(`[PuppeteerWorker] Failed to create tab ${i}:`, error.message);
            }
        }
        
        console.log(`[PuppeteerWorker] ✅ ${this.tabs.length} tabs created`);
    }

    async handleMessage(message) {
        if (!message || !message.type) return;
        
        console.log(`[PuppeteerWorker ${this.id}] Received: ${message.type}`);
        
        switch (message.type) {
            // ✅ Only handle SCRAPE_REQUEST from orchestrator
            case 'SCRAPE_REQUEST':
                await this.handleScrapeRequest(message);
                break;
                
            case 'SHUTDOWN':
                await this.shutdown();
                break;
                
            default:
                console.log(`[PuppeteerWorker ${this.id}] Unknown: ${message.type}`);
        }
    }

    async handleScrapeRequest(message) {
        const { messageId, payload, sourceWorkerId } = message;
        const { jobId, url, exchange, contract, eventData, analysis } = payload || {};
        
        console.log(`[PuppeteerWorker ${this.id}] 📥 Scraping: ${jobId} (${url})`);
        
        try {
            // Get available tab
            let tab = this.getAvailableTab();
            if (!tab) {
                tab = await this.waitForAvailableTab();
                if (!tab) {
                    throw new Error('No tabs available');
                }
            }
            
            // Scrape with tab
            const scrapedData = await this.scrapeWithTab(tab, url, jobId, exchange, contract);
            
            // ✅ Send response back via parentPort (orchestrator will handle it)
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SCRAPE_RESPONSE',
                    messageId: messageId,
                    payload: {
                        jobId: jobId,
                        url: url,
                        exchange: exchange,
                        contract: contract,
                        scrapedData: scrapedData,
                        success: true,
                        timestamp: Date.now()
                    }
                });
                console.log(`[PuppeteerWorker ${this.id}] 📤 Sent SCRAPE_RESPONSE for ${jobId}`);
            }
            
        } catch (error) {
            console.error(`[PuppeteerWorker ${this.id}] ❌ Scrape failed:`, error.message);
            
            if (parentPort) {
                parentPort.postMessage({
                    type: 'SCRAPE_RESPONSE',
                    messageId: messageId,
                    payload: {
                        jobId: jobId,
                        url: url,
                        exchange: exchange,
                        contract: contract,
                        error: error.message,
                        success: false,
                        timestamp: Date.now()
                    }
                });
            }
        }
    }

    getAvailableTab() {
        for (const tab of this.tabPool) {
            if (tab.status === 'idle') {
                tab.status = 'busy';
                console.log(`[PuppeteerWorker ${this.id}] ✅ Got tab: ${tab.id}`);
                return tab;
            }
        }
        return null;
    }

    async waitForAvailableTab(timeout = 30000) {
        const start = Date.now();
        while (Date.now() - start < timeout) {
            const tab = this.getAvailableTab();
            if (tab) return tab;
            await this.sleep(100);
        }
        return null;
    }

    async scrapeWithTab(tab, url, jobId, exchange, contract) {
        console.log(`[PuppeteerWorker ${this.id}] 🌐 Scraping ${jobId} on ${tab.id}`);
        
        if (!url) {
            tab.status = 'idle';
            throw new Error('No URL provided');
        }

        const page = tab.page;
        
        try {
            const currentUrl = page.url();
            if (currentUrl !== url && currentUrl !== 'about:blank') {
                await page.goto(url, { waitUntil: 'networkidle2', timeout: this.timeout });
            } else if (currentUrl === url) {
                await page.reload({ waitUntil: 'networkidle2' });
            } else {
                await page.goto(url, { waitUntil: 'networkidle2', timeout: this.timeout });
            }
            
            const metadata = await page.evaluate(() => {
                const title = document.title || '';
                const metaTags = {};
                const metaElements = document.querySelectorAll('meta');
                for (const meta of metaElements) {
                    const name = meta.getAttribute('name') || meta.getAttribute('property') || '';
                    const content = meta.getAttribute('content') || '';
                    if (name && content) metaTags[name] = content;
                }
                
                const ogTags = {};
                const ogElements = document.querySelectorAll('meta[property^="og:"]');
                for (const og of ogElements) {
                    const property = og.getAttribute('property') || '';
                    const content = og.getAttribute('content') || '';
                    if (property && content) ogTags[property] = content;
                }
                
                return {
                    title: title,
                    description: metaTags['description'] || ogTags['og:description'] || null,
                    keywords: metaTags['keywords'] || null,
                    language: document.documentElement.getAttribute('lang') || null,
                    robots: metaTags['robots'] || null,
                    og: ogTags,
                    meta: metaTags,
                };
            });
            
            // ✅ Release tab back to pool
            tab.status = 'idle';
            tab.currentUrl = url;
            tab.lastUsed = Date.now();
            tab.stats.scraped++;
            
            console.log(`[PuppeteerWorker ${this.id}] ✅ Scraped ${jobId}`);
            
            return {
                success: true,
                url: url,
                jobId: jobId,
                exchange: exchange || 'UNKNOWN',
                contract: contract || 'UNKNOWN',
                metadata: metadata,
                summary: {
                    title: metadata.title,
                    description: metadata.description,
                    hasOg: Object.keys(metadata.og || {}).length > 0,
                },
                tabId: tab.id,
                scrapedAt: Date.now()
            };
            
        } catch (error) {
            tab.status = 'idle';
            throw error;
        }
    }

    async recycleTab(tab) {
        console.log(`[PuppeteerWorker ${this.id}] 🔄 Recycling tab: ${tab.id}`);
        try {
            await tab.page.close();
            const page = await this.browser.newPage();
            page.setDefaultTimeout(this.timeout);
            page.setDefaultNavigationTimeout(this.timeout);
            await page.goto('about:blank', { waitUntil: 'load' });
            tab.page = page;
            tab.status = 'idle';
            tab.currentUrl = null;
            tab.lastUsed = Date.now();
            console.log(`[PuppeteerWorker ${this.id}] ✅ Tab recycled`);
        } catch (error) {
            console.error(`[PuppeteerWorker ${this.id}] ❌ Failed to recycle tab:`, error.message);
            const index = this.tabPool.indexOf(tab);
            if (index !== -1) this.tabPool.splice(index, 1);
        }
    }

    sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

    async shutdown() {
        console.log(`[PuppeteerWorker ${this.id}] Shutting down...`);
        this.isRunning = false;
        this.isReady = false;
        
        for (const tab of this.tabs) {
            try { await tab.page.close(); } catch (err) {}
        }
        
        if (this.browser) {
            try { await this.browser.close(); } catch (err) {}
        }
        
        this.tabs = [];
        this.tabPool = [];
        
        if (parentPort) {
            parentPort.postMessage({
                type: 'worker.shutdown',
                workerId: this.id,
                timestamp: Date.now()
            });
        }
    }
}

if (require.main === module) new PuppeteerWorker();
module.exports = PuppeteerWorker;