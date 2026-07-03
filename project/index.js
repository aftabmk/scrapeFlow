const main = require('./main');

if(require.main === module) {
	require('dotenv').config();
	main();
}

module.exports.handler = main;