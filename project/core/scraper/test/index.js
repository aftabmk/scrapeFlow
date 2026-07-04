const path = require('path');
const { ensureWSChild, ensureBrowserScrape } = require('./processManager.js');

async function main() {
  await ensureWSChild();

  try {
    const data = await ensureBrowserScrape();
    console.dir({ data }, { depth: 3 });
    return data;
  } catch (err) {
    console.error(err);
    throw err;
  }
}

module.exports.handler = main;

if (require.main === module) {
  require('dotenv').config({ path: path.resolve(__dirname, '../../../.env') });
  main();
}