# Enable performance monitoring
PROFILE=true node --experimental-sqlite index.js

# With CPU profiling
PROFILE=true node --cpu-prof --experimental-sqlite index.js

# With heap profiling
PROFILE=true node --heap-prof --experimental-sqlite index.js