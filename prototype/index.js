const TracerClass    = require('./class/tracerClass');
const JobClass       = require('./class/jobClass');
const WorkflowClass  = require('./class/workflowClass');
const ScraperClass   = require('./class/scraperClass');
const EvaluatorClass = require('./class/evaluatorClass');

// Tracer must be first — subscribes before any events can fire
const tracer    = new TracerClass();

const evaluator = new EvaluatorClass();   // listens to: scraperEvent
const scraper   = new ScraperClass();     // listens to: workflowEvent
const workflow  = new WorkflowClass();    // listens to: jobEvent
const job       = new JobClass();         // emits:       jobEvent

// Fire — triggers the full chain

Promise.all([
	job.run({ type: 'equity', symbol: 'AAPL' }),
	job.run({ type: 'future', symbol: 'BAT' })
]);