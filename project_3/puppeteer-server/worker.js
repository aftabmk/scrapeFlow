// puppeteer-server/worker.js
const { parentPort, workerData } = require('worker_threads');
const puppeteer = require('puppeteer');
const { namedMutex } = require('../utils/mutex');

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
        
        // ✅ Track which job has which tab
        this.tabAssignments = new Map();
        this.processingJobs = new Set();
        this.pendingRequests = new Map();
        
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
                    assignedJobId: null,
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

    /**
     * ✅ Handle scrape request - ONLY lock tab assignment, NOT scraping
     */
    async handleScrapeRequest(message) {
        const { messageId, payload, sourceWorkerId } = message;
        const { jobId, url, exchange, contract } = payload || {};
        
        console.log(`[PuppeteerWorker ${this.id}] 📥 Scraping: ${jobId}`);

        let tab = null;
        let isDuplicate = false;

        // ✅ STEP 1: Acquire mutex ONLY for tab assignment (FAST)
        await namedMutex.execute(`tab_pool_${this.id}`, async () => {
            // ✅ Check if job already being processed
            if (this.processingJobs.has(jobId)) {
                console.log(`[PuppeteerWorker ${this.id}] ⚠️ Job ${jobId} already processing`);
                isDuplicate = true;
                return;
            }
            
            // ✅ Get available tab (atomic operation)
            const availableTab = this.getAvailableTab(jobId);
            if (!availableTab) {
                console.log(`[PuppeteerWorker ${this.id}] ⚠️ No tabs available for ${jobId}`);
                return;
            }
            
            // ✅ Mark job as processing atomically
            this.processingJobs.add(jobId);
            availableTab.assignedJobId = jobId;
            this.tabAssignments.set(jobId, availableTab.id);
            tab = availableTab;
            
            console.log(`[PuppeteerWorker ${this.id}] ✅ Assigned tab ${tab.id} to ${jobId}`);
            console.log(`[PuppeteerWorker ${this.id}] 📊 Active: ${Array.from(this.processingJobs).join(', ')}`);
        });

        // ✅ Handle duplicate
        if (isDuplicate) {
            this.sendResponse(messageId, {
                jobId: jobId,
                duplicate: true,
                error: 'Already processing',
                success: false
            });
            return;
        }
        
        // ✅ Handle no tab
        if (!tab) {
            this.sendResponse(messageId, {
                jobId: jobId,
                error: 'No tabs available',
                success: false
            });
            return;
        }

        // ✅ STEP 2: Scrape WITH MUTEX RELEASED (CONCURRENT)
        // Mutex is released - other jobs can get other tabs!
        try {
            console.log(`[PuppeteerWorker ${this.id}] 🌐 Scraping ${jobId} on ${tab.id} (concurrent)`);
            
            const scrapedData = await this.scrapeWithTab(tab, url, jobId, exchange, contract);
            
            this.sendResponse(messageId, {
                jobId: jobId,
                url: url,
                exchange: exchange,
                contract: contract,
                scrapedData: scrapedData,
                success: true
            });
            
            console.log(`[PuppeteerWorker ${this.id}] ✅ Completed ${jobId} on ${tab.id}`);
            
        } catch (error) {
            console.error(`[PuppeteerWorker ${this.id}] ❌ Scrape failed:`, error.message);
            
            this.sendResponse(messageId, {
                jobId: jobId,
                error: error.message,
                success: false
            });
            
        } finally {
            // ✅ STEP 3: Release tab (ACQUIRE MUTEX AGAIN - FAST)
            await namedMutex.execute(`tab_pool_${this.id}`, () => {
                this.processingJobs.delete(jobId);
                this.tabAssignments.delete(jobId);
                if (tab) {
                    tab.assignedJobId = null;
                    tab.status = 'idle';
                }
                console.log(`[PuppeteerWorker ${this.id}] ✅ Released ${jobId}`);
                console.log(`[PuppeteerWorker ${this.id}] 📊 Active: ${Array.from(this.processingJobs).join(', ')}`);
            });
        }
    }

    /**
     * ✅ Get available tab with atomic check
     */
    getAvailableTab(jobId) {
        // Check if job already has a tab
        if (this.tabAssignments.has(jobId)) {
            const existingTabId = this.tabAssignments.get(jobId);
            const existingTab = this.tabPool.find(t => t.id === existingTabId);
            if (existingTab && existingTab.status === 'busy') {
                console.log(`[PuppeteerWorker ${this.id}] Job ${jobId} already on ${existingTabId}`);
                return null;
            }
        }
        
        // Find idle tab
        for (const tab of this.tabPool) {
            if (tab.status === 'idle' && !tab.assignedJobId) {
                tab.status = 'busy';
                console.log(`[PuppeteerWorker ${this.id}] ✅ Got tab: ${tab.id} for ${jobId}`);
                return tab;
            }
        }
        
        // Log tab statuses
        const statuses = this.tabPool.map(t => `${t.id}:${t.status}(${t.assignedJobId || 'none'})`).join(', ');
        console.log(`[PuppeteerWorker ${this.id}] 📊 Tab statuses: ${statuses}`);
        return null;
    }

    /**
     * Send response back
     */
    sendResponse(messageId, payload) {
        if (parentPort) {
            parentPort.postMessage({
                type: 'SCRAPE_RESPONSE',
                messageId: messageId,
                payload: payload,
                timestamp: Date.now()
            });
        }
    }

    /**
     * ✅ Scrape with tab - releases tab back to pool
     */
    async scrapeWithTab(tab, url, jobId, exchange, contract) {
        console.log(`[PuppeteerWorker ${this.id}] 🌐 Scraping ${jobId} on ${tab.id}`);
        
        if (!url) {
            tab.status = 'idle';
            tab.assignedJobId = null;
            throw new Error('No URL provided');
        }

        const page = tab.page;
        
        try {
            const currentUrl = page.url();
            if (currentUrl !== url && currentUrl !== 'about:blank') {
                console.log(`[PuppeteerWorker ${this.id}] 🔄 Navigating to ${url} on ${tab.id}`);
                await page.goto(url, { waitUntil: 'networkidle2', timeout: this.timeout });
            } else if (currentUrl === url) {
                console.log(`[PuppeteerWorker ${this.id}] ♻️ Reusing existing page on ${tab.id}`);
                await page.reload({ waitUntil: 'networkidle2' });
            } else {
                console.log(`[PuppeteerWorker ${this.id}] 🌐 Navigating to ${url} on ${tab.id}`);
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
            
            // ✅ Release tab back to pool (status already set in finally block)
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
            console.error(`[PuppeteerWorker ${this.id}] ❌ Error scraping ${jobId}:`, error.message);
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
            tab.assignedJobId = null;
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
        this.processingJobs.clear();
        this.tabAssignments.clear();
        
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