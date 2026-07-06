// math_engine_wrapper.js
const shadowEngine = require('./build/math_engine.js');

let calcPointer = null;

// 1. Create a Promise that resolves only when Wasm is 100% ready
const initCalculator = new Promise((resolve) => {
    shadowEngine.onRuntimeInitialized = () => {
        // Instantiate the C++ class on the Wasm heap once
        calcPointer = shadowEngine._createCalculator();
        resolve();
    };
});

// 2. The modular runMath function you wanted
function runMath(operation, num1, num2) {
    if (!calcPointer) {
        throw new Error("Calculator engine is not initialized yet. Wait for initCalculator.");
    }

    // Allocate temporary string on the stack for the operation name
    const opStringPointer = shadowEngine.allocate(
        shadowEngine.intArrayFromString(operation), 
        shadowEngine.ALLOC_STACK
    );

    // Execute the native C++ function code path
    return shadowEngine._executeOperation(calcPointer, opStringPointer, num1, num2);
}

// 3. Clean up method to prevent memory leaks when shutting down your app/server
function closeCalculator() {
    if (calcPointer) {
        shadowEngine._destroyCalculator(calcPointer);
        calcPointer = null;
    }
}

// Export the init promise, the executor, and the destroyer
module.exports = {
    initCalculator,
    runMath,
    closeCalculator
};
