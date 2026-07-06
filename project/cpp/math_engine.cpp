// math_engine.cpp
#include <emscripten.h>
#include <string>

extern "C" {
    EMSCRIPTEN_KEEPALIVE
    const char* addNumbersAndReturnString(int a, int b) {
        // 1. Calculate the sum
        int sum = a + b;

        // 2. Convert the sum to a standard C++ string
        std::string sumStr = std::to_string(sum);

        // 3. CRITICAL: Allocate persistent memory on the heap using 'new char[]'.
        // If you return sumStr.c_str() directly, the memory will be destroyed 
        // as soon as this function finishes executing, leading to garbage data in JS.
        char* buffer = new char[sumStr.length() + 1];
        std::strcpy(buffer, sumStr.c_str());

        // 4. Return the memory pointer address to JavaScript
        return buffer;
    }

    // 5. MEMORY SAFETY HELPER: JavaScript must call this function after reading 
    // the string to clear the memory, otherwise you will cause a memory leak!
    EMSCRIPTEN_KEEPALIVE
    void freeStringMemory(const char* pointer) {
        delete[] pointer;
    }
}
