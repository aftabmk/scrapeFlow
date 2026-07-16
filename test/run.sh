#!/usr/bin/env bash

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

print_header() {
    echo ""
    echo "${BLUE}╔══════════════════════════════════════════════════════════╗${NC}"
    echo "${BLUE}║           SCRAPE FLOW - Job-First Pipeline             ║${NC}"
    echo "${BLUE}╚══════════════════════════════════════════════════════════╝${NC}"
    echo ""
}

print_option() { echo "${CYAN}  $1) $2${NC}"; }

check_node() {
    local version=$(node -v 2>/dev/null | cut -d'v' -f2)
    if [ -z "$version" ]; then echo "${RED}❌ Node.js not installed${NC}"; exit 1; fi
    local major=$(echo $version | cut -d'.' -f1)
    if [ $major -lt 18 ]; then echo "${YELLOW}⚠️  Node $version detected, recommended 18+${NC}"; else echo "${GREEN}✅ Node $version${NC}"; fi
}

check_deps() {
    if [ ! -d "node_modules" ]; then echo "${YELLOW}⚠️  Installing dependencies...${NC}"; npm install; fi
}

ensure_data() { mkdir -p data logs exports performance/metrics; }

run_normal() { echo "${GREEN}▶ Running normal mode${NC}"; echo "──────────────────────────────────────"; node --experimental-worker index.js; }
run_dev() { echo "${GREEN}▶ Running development mode${NC}"; echo "──────────────────────────────────────"; node --experimental-worker --watch index.js; }
run_profile() { echo "${GREEN}▶ Running with profiling${NC}"; echo "──────────────────────────────────────"; PROFILE=true node --experimental-worker index.js; }
run_test() { echo "${GREEN}▶ Running test${NC}"; echo "──────────────────────────────────────"; node --experimental-worker test.js; }
run_quick() { echo "${GREEN}▶ Running quick test${NC}"; echo "──────────────────────────────────────"; node --experimental-worker test.js --quick; }

run_clean() {
    echo "${YELLOW}⚠️  Cleaning data...${NC}"
    read -p "Delete all data? (y/N) " -n 1 -r; echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then rm -rf data logs exports performance/metrics; echo "${GREEN}✅ Cleaned${NC}"; fi
}

show_help() {
    print_header
    echo "${BOLD}Options:${NC}"
    echo ""
    print_option "1" "Normal run"
    print_option "2" "Development mode (watch)"
    print_option "3" "Performance profiling"
    print_option "4" "Run test"
    print_option "5" "Run quick test"
    print_option "6" "Clean data"
    print_option "h" "Help"
    print_option "q" "Quit"
    echo ""
}

main_menu() {
    while true; do
        print_header
        echo "${BOLD}Select option:${NC}"
        echo ""
        print_option "1" "Normal run"
        print_option "2" "Development mode"
        print_option "3" "Performance profiling"
        print_option "4" "Run test"
        print_option "5" "Quick test"
        print_option "6" "Clean data"
        print_option "h" "Help"
        print_option "q" "Quit"
        echo ""
        echo -n "Choice: "
        read choice

        case $choice in
            1) check_node; check_deps; ensure_data; run_normal; break ;;
            2) check_node; check_deps; ensure_data; run_dev; break ;;
            3) check_node; check_deps; ensure_data; run_profile; break ;;
            4) check_node; check_deps; ensure_data; run_test; break ;;
            5) check_node; check_deps; ensure_data; run_quick; break ;;
            6) run_clean ;;
            h) show_help ;;
            q) echo "Goodbye!"; exit 0 ;;
            *) echo "${RED}Invalid option${NC}" ;;
        esac
        echo ""
    done
}

if [ $# -gt 0 ]; then
    case $1 in
        1) check_node; check_deps; ensure_data; run_normal ;;
        2) check_node; check_deps; ensure_data; run_dev ;;
        3) check_node; check_deps; ensure_data; run_profile ;;
        4) check_node; check_deps; ensure_data; run_test ;;
        5) check_node; check_deps; ensure_data; run_quick ;;
        6) run_clean ;;
        -h|--help) show_help ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
else
    main_menu
fi