// recover.js still owns the replay logic — it just no longer touches
// sqlite directly. WalServer holds the DatabaseSync connection and only
// answers "give me the raw rows for this queue"; recover.js does the
// append/deliver/ack reduction into active jobs, same as before.
async function recover(bus, queue) {
    const { rows } = await bus.request({
        op: "recover",
        queue: queue.name
    });

    const active = new Map();
    const delivered = new Set();

    for (const row of rows) {

        switch (row.op) {

            case "append":
                active.set(
                    row.id,
                    JSON.parse(row.payload)
                );
                break;

            case "deliver":
                delivered.add(row.id);
                break;

            case "ack":
                active.delete(row.id);
                delivered.delete(row.id);
                break;
        }
    }

    for (const job of active.values())
        queue._pushBack(job);
}

module.exports = recover;