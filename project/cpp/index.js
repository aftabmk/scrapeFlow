// app.js
const { initCalculator, runMath, closeCalculator } = require('./wrapper.js');

const main = async () => {
    try {
        console.log("Initializing high-performance C++ Wasm engine...");
        
        // 1. CRITICAL: Wait for the WebAssembly runtime to stand up
        await initCalculator;
        
        console.log("Engine ready! Running calculations...");

        // 2. Call your exported runMath function seamlessly 
        const addition = runMath("add", 10.5, 5.25);
        const division = runMath("divide", 10, 3.6);
        const multiplication = runMath("multiply", 4, 2.5);

        console.log("Addition Result:", addition);       // Output: 15.75
        console.log("Division Result:", division);       // Output: 2.7777777777777777
        console.log("Multiplication Result:", multiplication); // Output: 10

    } catch (error) {
        console.error("Execution failure:", error);
    } finally {
        // 3. Optional: Free up memory if your script is completely finished
        closeCalculator();
    }
}

main();
