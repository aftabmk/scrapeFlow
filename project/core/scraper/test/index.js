const path = require('path');
const main = require('./main');

module.exports.handler = main;

if (require.main === module) {
  require('dotenv').config({ path: path.resolve(__dirname, '..','..','..','.env') });
  main();
}