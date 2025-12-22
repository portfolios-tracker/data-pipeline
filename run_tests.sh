#!/bin/bash

# Test Runner Script for Fin-Sight
# This script helps run tests in various configurations

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
COVERAGE=true
VERBOSE=false
MARKERS=""
SPECIFIC_FILE=""

# Help message
show_help() {
    cat << EOF
Usage: ./run_tests.sh [OPTIONS]

Options:
    -h, --help              Show this help message
    -f, --file FILE         Run tests from specific file
    -m, --marker MARKER     Run tests with specific marker (unit, integration, slow)
    -n, --no-coverage       Skip coverage report
    -v, --verbose           Verbose output
    --unit                  Run only unit tests
    --integration           Run only integration tests
    --docker                Run tests inside Docker container

Examples:
    ./run_tests.sh                              # Run all tests with coverage
    ./run_tests.sh --unit                       # Run only unit tests
    ./run_tests.sh -f tests/unit/test_fetcher.py  # Run specific file
    ./run_tests.sh --docker                     # Run in Docker container
    ./run_tests.sh -v --no-coverage             # Verbose without coverage

EOF
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -f|--file)
            SPECIFIC_FILE="$2"
            shift 2
            ;;
        -m|--marker)
            MARKERS="-m $2"
            shift 2
            ;;
        -n|--no-coverage)
            COVERAGE=false
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        --unit)
            MARKERS="-m unit"
            shift
            ;;
        --integration)
            MARKERS="-m integration"
            shift
            ;;
        --docker)
            echo -e "${YELLOW}Running tests in Docker container...${NC}"
            docker exec -it fin-sight-airflow-worker-1 bash -c "cd /opt/airflow && pytest $@"
            exit $?
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Build pytest command
PYTEST_CMD="pytest"

# Add specific file if provided
if [ -n "$SPECIFIC_FILE" ]; then
    PYTEST_CMD="$PYTEST_CMD $SPECIFIC_FILE"
fi

# Add markers if provided
if [ -n "$MARKERS" ]; then
    PYTEST_CMD="$PYTEST_CMD $MARKERS"
fi

# Add verbose flag
if [ "$VERBOSE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD -vv"
fi

# Add coverage options
if [ "$COVERAGE" = true ]; then
    PYTEST_CMD="$PYTEST_CMD --cov=dags --cov-report=term-missing --cov-report=html"
fi

# Print header
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}    Fin-Sight Test Suite Runner      ${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo -e "Command: ${YELLOW}$PYTEST_CMD${NC}"
echo ""

# Run tests
$PYTEST_CMD

# Check exit code
TEST_EXIT_CODE=$?

echo ""
echo -e "${GREEN}======================================${NC}"

if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
    
    if [ "$COVERAGE" = true ]; then
        echo -e "${GREEN}✓ Coverage report generated: htmlcov/index.html${NC}"
    fi
else
    echo -e "${RED}✗ Some tests failed!${NC}"
    echo -e "${RED}Exit code: $TEST_EXIT_CODE${NC}"
fi

echo -e "${GREEN}======================================${NC}"

exit $TEST_EXIT_CODE
