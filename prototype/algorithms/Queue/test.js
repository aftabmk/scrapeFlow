const EventQueue = require("./algorithms/EventQueue");
const JobEvent = require("../../events/jobEvent");

const queue = new EventQueue(JobEvent);

// Produce events
JobEvent.emit({ id: "1", type: "job.created", payload: { name: "Job 1" } });
JobEvent.emit({ id: "2", type: "job.running", payload: { name: "Job 2" } });
JobEvent.emit({ id: "3", type: "job.completed", payload: { name: "Job 3" } });

// Consumer
const main = async () => {
  for (;;) {
    const messages = await queue.receiveMessages();

    for (const msg of messages) {
      const event = JSON.parse(msg.Body);

      console.log("Received:", event);

      await queue.deleteMessage(msg.ReceiptHandle);
    }
  }
};

main();