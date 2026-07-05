const Bus = require("./server/bus.js");
const DurableQueue = require("./algorithm/DurableQueue.js");

// const recover = require("./models/recover.js");

const [,,mode] = process.argv;

const main = async() => {
    const bus = new Bus();

    const queue = new DurableQueue("email" , {
        visibilityTimeout: 1,
        maxRetries: 3
    });

	await queue.recover();
	
    switch (mode) {
        case "seed":
            await queue.enqueue({ id: 1, email: "a@gmail.com" });
            await queue.enqueue({ id: 2, email: "b@gmail.com" });

            console.log("seeded 2 jobs, pending size:", queue.size());
            break;

        case "crash": {
            // await recover(bus, queue);

            console.log("after recover, queue size:", queue.size());
            console.log("recovered job ids:", [...queue.pending.keys()]);

            const job = await queue.dequeue();

            if (!job) {
                console.log("nothing to dequeue, can't simulate crash");
                break;
            }

            console.log("dequeued", job.id, "- simulating crash BEFORE ack");

            // intentionally crash
            process.exit(1);
        }

        case "recover":
            // await recover(bus, queue);

            console.log("after recover, queue size:", queue.size());
            console.log("recovered job ids:", [...queue.pending.keys()]);

            while (!queue.empty()) {
                const job = await queue.dequeue();

                if (!job)
                    break;

                console.log("processing", job);

                const ok = await queue.ack(job.id);
                // test
                // const ok = await queue.ack(4);
                // giving fake id, keeps the job in the queue and will fetch over and over again
                console.log("job acknowledged = job id : %d, status : %s", job.id, ok);
            }

            break;

        default:
            console.log("pass a mode: seed | crash | recover");
    }

}

main().catch(err => {
    console.error(err);
    process.exit(1);
});

// start server in sqlite/server/walServer
