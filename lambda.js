// 1. Define your Lambda function named "main"
const main = async (event, context) => {
    // This logs the local mock payload to your terminal
    console.log("Received event payload:\n", JSON.stringify(event, null, 2));

    const response = {
        statusCode: 200,
        body: JSON.stringify({ message: "Hello from local Lambda!" }),
    };

    return response;
};

// 2. Export the function using standard CommonJS syntax
module .exports .handler = main;

// ==========================================
// LOCAL TESTING ENVIRONMENT
// ==========================================

// Create your mock event payload
const mockEvent = {
    key1: "value1",
    key2: "value2",
    body: JSON.stringify({ user: "John Doe" })
};

// Create a basic mock context object
const mockContext = {
    functionName: "local-lambda-test",
    awsRequestId: "local-id-12345"
};

// Manually execute the function locally and log the final output
main(mockEvent, mockContext)
    .then((result) => {
        console.log("\nExecution Result:\n", JSON.stringify(result, null, 2));
    })
    .catch((error) => {
        console.error("\nExecution Error:\n", error);
    });
