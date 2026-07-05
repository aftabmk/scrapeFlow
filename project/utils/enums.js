const NODES = Object.freeze({
  JOBS : 'jobs',
  QUEUE : 'queue',
  BROWSER : 'browser',
  WALSERVER : 'walServer',
});

const PATH = Object.freeze({
  JOBS : './process/jobsProcess.js',
  QUEUE : './process/queueProcess.js',
  BROWSER : './process/browserProcess.js',
  WALSERVER : './algorithms/sqlite/server',
});

const DEPENDENCY = Object.freeze({
  BROWSER : [],
  WALSERVER : [],
  JOBS : [NODES.QUEUE],
  QUEUE : [NODES.BROWSER,NODES.WALSERVER],
});

module.exports = { NODES, PATH, DEPENDENCY };