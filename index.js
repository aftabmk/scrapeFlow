
//  index.js — Application bootstrap and event wiring

//  Full event flow:

//    produceJobs()
//        │
//        ▼ eventBus.publish('jobproducer:job:created')
//    JobConsumer._handleJobCreated()
//        │
//        ▼ eventBus.publish('browser:request', BrowserEvent)
//    Browser.handleEvent()
//        │
//        ├─ success ──▶ eventBus.publish('browser:response', { pageId, result })
//        │
//        └─ failure ──▶ eventBus.publish('browser:dlq', DLQEvent)
//                            │
//                            ▼  DLQ handler (this file)
//                            recoveryAction() → wait() → recordAttempt()
//                            │
//                            ├─ canRetry  ──▶ eventBus.publish('browser:request', ...)
//                            └─ exhausted ──▶ eventBus.publish('browser:dead', ...)

//  Usage (Lambda handler or local runner):

const { bootstrap, produceJobs, eventBus } = require('./src');

const main = async() => {
	await bootstrap();        // init browser + start consumer
	produceJobs('prod-1');    // fire all jobs onto the bus
	
	// optional — listen for results
	eventBus.subscribe('browser:response', ({ data }) => console.log(data));
	eventBus.subscribe('browser:dead',     ({ data }) => console.error(data));
	
}

console.log(process.argv);
main();