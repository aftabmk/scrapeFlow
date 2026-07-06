// math_engine.cpp
#include <emscripten.h>
#include <cstring>

class Calculator {
public:
    double add(double a, double b) { return a + b; }
    double subtract(double a, double b) { return a - b; }
    double multiply(double a, double b) { return a * b; }
    
    double divide(double a, double b) {
        if (b == 0) return 0; // Prevent crash by zero division
        return a / b;
    }
};

extern "C" {
    // 1. Factory constructor pattern to instantiate the object
    EMSCRIPTEN_KEEPALIVE
    Calculator* createCalculator() {
        return new Calculator();
    }

    // 2. Performance Controller. Accepts and returns clean doubles.
    EMSCRIPTEN_KEEPALIVE
    double executeOperation(Calculator* calc, const char* op, double a, double b) {
        if (!calc) return 0;

        if (std::strcmp(op, "add") == 0) return calc->add(a, b);
        if (std::strcmp(op, "subtract") == 0) return calc->subtract(a, b);
        if (std::strcmp(op, "multiply") == 0) return calc->multiply(a, b);
        if (std::strcmp(op, "divide") == 0) return calc->divide(a, b);

        return 0; // Return 0 if operation string is invalid
    }

    // 3. Destructor to prevent memory leak of the object instance
    EMSCRIPTEN_KEEPALIVE
    void destroyCalculator(Calculator* calc) {
        delete calc;
    }
}

