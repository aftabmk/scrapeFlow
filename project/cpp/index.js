// test.js
const shadowEngine = require('./build/math_engine.js');

shadowEngine.onRuntimeInitialized = () => {
    // 1. Call the function. It returns a number representing the memory address pointer.
    const stringPointer = shadowEngine._addNumbersAndReturnString(50, 25);
    
    // 2. Read the actual characters from that memory pointer address
    const resultString = shadowEngine.UTF8ToString(stringPointer);
    
    console.log("Result received from C++ as a JS string:", resultString);
    console.log("Type of result:", typeof resultString); // Output: string

    // 3. Free up the C++ memory heap to prevent memory leaks
    shadowEngine._freeStringMemory(stringPointer);
};
