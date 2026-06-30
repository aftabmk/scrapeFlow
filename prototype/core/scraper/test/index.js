require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });

const { launchBrowser, createPage } = require ('./browser.js');
const { inject, triggerFetch } = require ('./scraper.js');
const {PAGE_URL_1,API_URL_1,PAGE_URL_3,API_URL_3} = process.env;

async function main() {
  try {
    const browser = await launchBrowser();
    
    const page1 = await createPage(browser);
    const page2 = await createPage(browser);

    await inject(page1, 8080, 'page1');
    await inject(page2, 8080, 'page2');
    
    await Promise.all([
      page1.goto(PAGE_URL_1, { waitUntil: 'domcontentloaded'}),
      page2.goto(PAGE_URL_3, {waitUntil: 'domcontentloaded'})
    ]);

    console.log('\n🎯 WebSocket Scraper Ready!');

    await Promise.all([
      triggerFetch(page1, API_URL_1),   
      triggerFetch(page2, API_URL_3) 
    ]);
    
  } 
  catch (error) {
    console.error('❌ Main Error:', error);
  }
}

main();