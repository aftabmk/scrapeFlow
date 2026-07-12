#!/usr/bin/env bash

# ============================================================
# SCRAPE FLOW - Interactive Runner
# ============================================================

set -e

# Color codes for pretty output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
MAGENTA='\033[0;35m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# ============================================================
# CONFIGURATION
# ============================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NODE_VERSION="22.16.0"
EXPERIMENTAL_FLAG="--experimental-sqlite"

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

print_header() {
    echo ""
    echo "${BOLD}${CYAN}╔════════════════════════════════════════════════════════════════╗${NC}"
    echo "${BOLD}${CYAN}║                    SCRAPE FLOW RUNNER                         ║${NC}"
    echo "${BOLD}${CYAN}╚════════════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_title() {
    echo "${BOLD}${GREEN}▶ $1${NC}"
}

print_info() {
    echo "${BLUE}ℹ $1${NC}"
}

print_success() {
    echo "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo "${YELLOW}⚠️ $1${NC}"
}

print_error() {
    echo "${RED}❌ $1${NC}"
}

print_option() {
    echo "${CYAN}  $1) $2${NC}"
}

print_separator() {
    echo "${BOLD}${MAGENTA}────────────────────────────────────────────────────────────────────${NC}"
}

check_node_version() {
    local current_version=$(node -v 2>/dev/null | cut -d'v' -f2)
    if [ -z "$current_version" ]; then
        print_error "Node.js is not installed!"
        print_info "Please install Node.js v$NODE_VERSION or higher"
        exit 1
    fi

    local major_version=$(echo $current_version | cut -d'.' -f1)
    if [ $major_version -lt 22 ]; then
        print_warning "Node.js version $current_version detected"
        print_info "Recommended version: v$NODE_VERSION or higher"
        print_info "Some features may not work properly"
    else
        print_success "Node.js version: v$current_version"
    fi
}

check_dependencies() {
    print_info "Checking dependencies..."
    
    if [ ! -f "package.json" ]; then
        print_error "package.json not found!"
        exit 1
    fi

    if [ ! -d "node_modules" ]; then
        print_warning "node_modules not found. Installing dependencies..."
        npm install
    else
        print_success "Dependencies installed"
    fi
}

ensure_data_dir() {
    if [ ! -d "data" ]; then
        mkdir -p data
        print_info "Created data directory"
    fi
}

ensure_performance_dir() {
    if [ ! -d "performance/metrics" ]; then
        mkdir -p performance/metrics
        print_info "Created performance/metrics directory"
    fi
}

# ============================================================
# RUN FUNCTIONS
# ============================================================

run_normal() {
    print_title "Running in NORMAL mode"
    print_info "Starting application without profiling..."
    print_separator
    node $EXPERIMENTAL_FLAG index.js
}

run_dev() {
    print_title "Running in DEVELOPMENT mode"
    print_info "Starting application with watch mode..."
    print_separator
    node $EXPERIMENTAL_FLAG --watch index.js
}

run_profile() {
    print_title "Running with PERFORMANCE PROFILING"
    print_info "Profile mode enabled. Metrics will be saved to performance/metrics/"
    print_separator
    PROFILE=true node $EXPERIMENTAL_FLAG index.js
}

run_test() {
    print_title "Running PERFORMANCE TEST"
    print_info "Running full performance test suite..."
    print_separator
    node $EXPERIMENTAL_FLAG test.js
}

run_test_quick() {
    print_title "Running QUICK PERFORMANCE TEST"
    print_info "Running quick performance test..."
    print_separator
    node $EXPERIMENTAL_FLAG test.js --quick
}

run_test_profile() {
    print_title "Running PERFORMANCE TEST with PROFILING"
    print_info "Profile mode enabled. Metrics will be saved to performance/metrics/"
    print_separator
    PROFILE=true node $EXPERIMENTAL_FLAG test.js
}

run_test_quick_profile() {
    print_title "Running QUICK PERFORMANCE TEST with PROFILING"
    print_info "Profile mode enabled. Metrics will be saved to performance/metrics/"
    print_separator
    PROFILE=true node $EXPERIMENTAL_FLAG test.js --quick
}

run_cpu_profile() {
    print_title "Running with CPU PROFILING"
    print_info "CPU profile will be saved to performance/metrics/"
    print_separator
    PROFILE=true node --cpu-prof --cpu-prof-dir=./performance/metrics $EXPERIMENTAL_FLAG index.js
}

run_heap_profile() {
    print_title "Running with HEAP PROFILING"
    print_info "Heap profile will be saved to performance/metrics/"
    print_separator
    PROFILE=true node --heap-prof --heap-prof-dir=./performance/metrics $EXPERIMENTAL_FLAG index.js
}

run_full_profile() {
    print_title "Running with FULL PROFILING (CPU + Heap)"
    print_info "CPU and Heap profiles will be saved to performance/metrics/"
    print_separator
    PROFILE=true node --cpu-prof --cpu-prof-dir=./performance/metrics --heap-prof --heap-prof-dir=./performance/metrics $EXPERIMENTAL_FLAG index.js
}

run_inspect() {
    print_title "Running with INSPECTOR"
    print_info "Open chrome://inspect in Chrome to attach debugger"
    print_separator
    node --inspect $EXPERIMENTAL_FLAG index.js
}

run_inspect_brk() {
    print_title "Running with INSPECTOR (break on start)"
    print_info "Open chrome://inspect in Chrome to attach debugger"
    print_info "Execution will pause until debugger attaches"
    print_separator
    node --inspect-brk $EXPERIMENTAL_FLAG index.js
}

run_clean() {
    print_title "Cleaning up"
    print_info "Removing data directory..."
    
    read -p "Are you sure you want to delete all data? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        if [ -d "data" ]; then
            rm -rf data
            print_success "Data directory removed"
        else
            print_info "No data directory found"
        fi
        
        if [ -d "performance/metrics" ]; then
            read -p "Delete performance metrics as well? (y/N) " -n 1 -r
            echo
            if [[ $REPLY =~ ^[Yy]$ ]]; then
                rm -rf performance/metrics
                print_success "Performance metrics removed"
            fi
        fi
    else
        print_info "Cleanup cancelled"
    fi
}

run_help() {
    print_header
    echo "${BOLD}AVAILABLE OPTIONS${NC}"
    echo ""
    print_option "1" "Normal run (no profiling)"
    print_option "2" "Development run (with watch)"
    print_option "3" "Performance profiling"
    print_option "4" "Performance test (full)"
    print_option "5" "Performance test (quick)"
    print_option "6" "Performance test with profiling (full)"
    print_option "7" "Performance test with profiling (quick)"
    print_option "8" "CPU profiling"
    print_option "9" "Heap profiling"
    print_option "10" "Full profiling (CPU + Heap)"
    print_option "11" "Inspector mode (chrome://inspect)"
    print_option "12" "Inspector mode with break on start"
    print_option "13" "Clean (remove data and metrics)"
    print_option "h" "Show this help"
    print_option "q" "Quit"
    echo ""
    print_separator
    echo ""
}

# ============================================================
# MAIN MENU
# ============================================================

main_menu() {
    while true; do
        print_header
        echo "${BOLD}${YELLOW}Select an option:${NC}"
        echo ""
        print_option "1" "Normal run"
        print_option "2" "Development run (watch)"
        print_option "3" "Performance profiling"
        print_option "4" "Performance test (full)"
        print_option "5" "Performance test (quick)"
        print_option "6" "Performance test with profiling (full)"
        print_option "7" "Performance test with profiling (quick)"
        print_option "8" "CPU profiling"
        print_option "9" "Heap profiling"
        print_option "10" "Full profiling (CPU + Heap)"
        print_option "11" "Inspector mode"
        print_option "12" "Inspector mode (break on start)"
        print_option "13" "Clean"
        print_option "h" "Help"
        print_option "q" "Quit"
        echo ""
        echo -n "${BOLD}Enter your choice: ${NC}"
        read choice

        case $choice in
            1)  check_node_version
                check_dependencies
                ensure_data_dir
                run_normal
                ;;
            2)  check_node_version
                check_dependencies
                ensure_data_dir
                run_dev
                ;;
            3)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_profile
                ;;
            4)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_test
                ;;
            5)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_test_quick
                ;;
            6)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_test_profile
                ;;
            7)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_test_quick_profile
                ;;
            8)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_cpu_profile
                ;;
            9)  check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_heap_profile
                ;;
            10) check_node_version
                check_dependencies
                ensure_data_dir
                ensure_performance_dir
                run_full_profile
                ;;
            11) check_node_version
                check_dependencies
                ensure_data_dir
                run_inspect
                ;;
            12) check_node_version
                check_dependencies
                ensure_data_dir
                run_inspect_brk
                ;;
            13) run_clean
                ;;
            h|H) run_help
                ;;
            q|Q) echo ""
                print_info "Goodbye!"
                exit 0
                ;;
            *)  print_error "Invalid option: $choice"
                echo "Press Enter to continue..."
                read
                ;;
        esac

        echo ""
        echo "Press Enter to return to menu..."
        read
        clear
    done
}

# ============================================================
# ENTRY POINT
# ============================================================

# Handle command line arguments
if [ $# -gt 0 ]; then
    case $1 in
        1)  check_node_version
            check_dependencies
            ensure_data_dir
            run_normal
            ;;
        2)  check_node_version
            check_dependencies
            ensure_data_dir
            run_dev
            ;;
        3)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_profile
            ;;
        4)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_test
            ;;
        5)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_test_quick
            ;;
        6)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_test_profile
            ;;
        7)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_test_quick_profile
            ;;
        8)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_cpu_profile
            ;;
        9)  check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_heap_profile
            ;;
        10) check_node_version
            check_dependencies
            ensure_data_dir
            ensure_performance_dir
            run_full_profile
            ;;
        11) check_node_version
            check_dependencies
            ensure_data_dir
            run_inspect
            ;;
        12) check_node_version
            check_dependencies
            ensure_data_dir
            run_inspect_brk
            ;;
        -h|--help) run_help
            ;;
        *)  echo "Unknown argument: $1"
            echo "Usage: ./run.sh [option]"
            echo "Or run without arguments for interactive menu"
            echo ""
            echo "Options:"
            echo "  1  - Normal run"
            echo "  2  - Development run"
            echo "  3  - Performance profiling"
            echo "  4  - Performance test (full)"
            echo "  5  - Performance test (quick)"
            echo "  6  - Performance test with profiling (full)"
            echo "  7  - Performance test with profiling (quick)"
            echo "  8  - CPU profiling"
            echo "  9  - Heap profiling"
            echo "  10 - Full profiling (CPU + Heap)"
            echo "  11 - Inspector mode"
            echo "  12 - Inspector mode (break on start)"
            echo "  -h, --help - Show help"
            exit 1
            ;;
    esac
else
    # Interactive mode
    main_menu
fi