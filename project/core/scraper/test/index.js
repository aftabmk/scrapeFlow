const path = require('path');
const { ensureWSChild, ensureBrowserScrape } = require('./process/processManager.js');

async function main() {
  try {
    await ensureWSChild();
    await ensureBrowserScrape();
  } 
  catch (err) {
    console.error(err);
    throw err;
  }
}

module.exports.handler = main;

if (require.main === module) {
  require('dotenv').config({ path: path.resolve(__dirname, '..','..','..','.env') });
  main();
}