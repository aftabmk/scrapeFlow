const TracerClass    = require('./core/tracer/tracerClass');
const JobClass       = require('./core/job/jobClass');
const WorkflowClass  = require('./core/workflow/workflowClass');
const ScraperClass   = require('./core/scraper/scraperClass');
const EvaluatorClass = require('./core/evaluator/evaluatorClass');

// Tracer must be first — subscribes before any events can fire
const tracer    = new TracerClass();

const evaluator = new EvaluatorClass();   // listens to: scraperEvent
const scraper   = new ScraperClass();     // listens to: workflowEvent
const workflow  = new WorkflowClass();    // listens to: jobEvent
const job       = new JobClass();         // emits:       jobEvent

// Fire — triggers the full chain

Promise.all([
	// job.run({ type: 'equity', symbol: 'AAPL' }),
	job.run({ type: 'future', symbol: 'BAT' })
]);