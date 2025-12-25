#!/bin/bash
# Script to run Python scripts inside Docker container

SCRIPT_NAME=$1
shift  # Remove first argument, rest are script arguments

if [ -z "$SCRIPT_NAME" ]; then
    echo "Usage: ./run_in_docker.sh <script_name> [script_arguments]"
    echo ""
    echo "Examples:"
    echo "  ./run_in_docker.sh upload_jsonl_to_s3.py"
    echo "  ./run_in_docker.sh upload_to_s3.py"
    echo "  ./run_in_docker.sh upload_large_jsonl.py ../meta_Amazon_Fashion.jsonl 10000"
    echo "  ./run_in_docker.sh upload_streaming.py ../meta_Amazon_Fashion.jsonl 1000 20"
    echo ""
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$( cd "$SCRIPT_DIR/.." && pwd )"

# Change to project directory
cd "$PROJECT_DIR"

# Check if script exists
if [ ! -f "scripts/$SCRIPT_NAME" ]; then
    echo "Error: Script 'scripts/$SCRIPT_NAME' not found"
    exit 1
fi

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi

# Check if airflow-worker container exists and is running
if ! docker compose ps airflow-worker 2>/dev/null | grep -q "Up"; then
    echo "Warning: airflow-worker container is not running."
    echo "Starting services..."
    docker compose up -d
    sleep 5
fi

echo "============================================================"
echo "Running script in Docker container"
echo "============================================================"
echo "Script: $SCRIPT_NAME"
if [ $# -gt 0 ]; then
    echo "Arguments: $@"
fi
echo ""

# Run script in airflow-worker container
# Use -it for interactive scripts, -T for non-interactive
if [[ "$SCRIPT_NAME" == *"upload"* ]] && [[ "$SCRIPT_NAME" != *"large"* ]]; then
    # Interactive scripts
    docker compose exec -it airflow-worker python /opt/airflow/scripts/$SCRIPT_NAME "$@"
else
    # Non-interactive scripts
    docker compose exec -T airflow-worker python /opt/airflow/scripts/$SCRIPT_NAME "$@"
fi

