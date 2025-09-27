#!/bin/bash

# CSV Worker System Startup Script
# This script helps you quickly start the CSV worker system

set -e

echo "==================================="
echo "CSV Worker System Startup"
echo "==================================="

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo "Error: Docker is not running or not accessible"
        echo "Please start Docker and try again"
        exit 1
    fi
}

# Function to create required directories
create_directories() {
    echo "Creating required directories..."
    mkdir -p loadingcsv output logs
    echo "✓ Directories created"
}

# Function to build and start the service
start_service() {
    echo "Building and starting CSV Worker API service..."

    # Use the updated docker-compose file
    if [ -f "docker-compose.yaml" ]; then
        docker-compose -f docker-compose.yaml down
        docker-compose -f docker-compose.yaml up --build -d
    else
        echo "Error: docker-compose.yaml not found"
        exit 1
    fi

    echo "✓ Service started successfully"
}

# Function to show service status
show_status() {
    echo ""
    echo "Service Status:"
    echo "==============="
    docker-compose -f docker-compose-updated.yaml ps

    echo ""
    echo "API Health Check:"
    echo "================="

    # Wait a moment for service to start
    sleep 3

    # Check if API is responding
    if curl -s http://localhost:5000/health > /dev/null; then
        echo "✓ API is running and healthy"
        echo "✓ Dashboard available at: http://localhost:5000/dashboard.html"
    else
        echo "⚠ API health check failed - service may still be starting"
        echo "Please wait a few moments and try: curl http://localhost:5000/health"
    fi
}

# Function to show usage information
show_usage() {
    echo ""
    echo "CSV Worker System is now running!"
    echo ""
    echo "Available endpoints:"
    echo "- Dashboard: http://localhost:5000/dashboard.html"
    echo "- API Health: http://localhost:5000/health"
    echo "- API Status: http://localhost:5000/status"
    echo "- Start Workers: POST http://localhost:5000/start"
    echo "- Stop Workers: POST http://localhost:5000/stop"
    echo "- List Files: http://localhost:5000/files"
    echo ""
    echo "File directories:"
    echo "- Watch folder: ./loadingcsv (place CSV files here)"
    echo "- Output folder: ./output (processed files)"
    echo "- Logs folder: ./logs (system logs)"
    echo ""
    echo "To stop the service: docker-compose -f docker-compose-updated.yaml down"
    echo "To view logs: docker-compose -f docker-compose-updated.yaml logs -f"
}

# Main execution
main() {
    case "${1:-start}" in
        "start"|"")
            check_docker
            create_directories
            start_service
            show_status
            show_usage
            ;;
        "stop")
            echo "Stopping CSV Worker System..."
            docker-compose -f docker-compose-updated.yaml down
            echo "✓ Service stopped"
            ;;
        "status")
            docker-compose -f docker-compose-updated.yaml ps
            curl -s http://localhost:5000/health | python3 -m json.tool 2>/dev/null || echo "API not responding"
            ;;
        "logs")
            docker-compose -f docker-compose-updated.yaml logs -f
            ;;
        "restart")
            echo "Restarting CSV Worker System..."
            docker-compose -f docker-compose-updated.yaml restart
            echo "✓ Service restarted"
            ;;
        *)
            echo "Usage: $0 {start|stop|status|logs|restart}"
            echo ""
            echo "Commands:"
            echo "  start   - Start the CSV worker system (default)"
            echo "  stop    - Stop the CSV worker system"
            echo "  status  - Show service status"
            echo "  logs    - Show and follow service logs"
            echo "  restart - Restart the service"
            exit 1
            ;;
    esac
}

main "$@"
