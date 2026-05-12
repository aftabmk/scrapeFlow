const TracerClass    = require('./core/tracerClass');
const JobClass       = require('./core/jobClass');
const WorkflowClass  = require('./core/workflowClass');
const ScraperClass   = require('./core/scraperClass');
const EvaluatorClass = require('./core/evaluatorClass');

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