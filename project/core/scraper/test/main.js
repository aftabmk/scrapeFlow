const path = require('path');
const { PipelineRunner  } = require('./process/PipelineRunner');

const workerDefinitions = [
  {
    node: 'ensureWSChild',
    path: path.join(__dirname, '.', 'workers', 'websocketWorker.js'),
    dependency: [],
  },
  {
    node: 'ensureBrowserScrape',
    path: path.join(__dirname, '.', 'workers', 'browserWorker.js'),
    dependency: ['ensureWSChild'],
  },
];

const main = async () => {
  await PipelineRunner.run(workerDefinitions);
};

module.exports = main;