// 1. Normal async fetch (already non-blocking)
async function fetchUrl(url) {
  console.log(`🌐 Fetching: ${url}`);
  const response = await fetch(url);
  const data = await response.json();
  console.log(`✅ Fetched data from ${url}`);
  return data;
}

// 2. Original BLOCKING function
function calculateBlocking() {
  console.log("🔢 Starting heavy calculation...");
  let sum = 0;
  for (let i = 1; i <= 1_000_000_000; i++) {
    sum += i * i;   // Some heavy work
  }
  console.log("✅ Calculation done, sum =", sum);
  return sum;
}

// 3. NON-BLOCKING version using Promise
function calculateNonBlocking() {
  return new Promise((resolve) => {
    console.log("🔢 Starting heavy calculation (non-blocking)...");

    // Use setImmediate / setTimeout to yield control back to event loop
    setImmediate(() => {
      let sum = 0;
      for (let i = 1; i <= 1_000_000_000; i++) {
        sum += i * i;
      }
      console.log("✅ Calculation done (non-blocking), sum =", sum);
      resolve(sum);        // Important: resolve when done
    });
  });
}

// 4. Main demo
async function main() {
  console.log("🚀 Program started");

  // These run in parallel (non-blocking)
  const fetchPromise = fetchUrl("https://jsonplaceholder.typicode.com/todos/1");
  const calcPromise = calculateNonBlocking();

//   const calcBlocking = calculateBlocking();
  console.log("📢 Hello from main thread (this prints immediately)");
  // Wait for both
  const [data, result] = await Promise.all([fetchPromise, calcPromise]);

  console.log("🎉 All done!");
  console.log("Fetch result:", data);
//   console.log("Calculate blocking result:", calcBlocking);
  console.log("Calculate non blocking result:", result);
}

// Run
main();