#!/bin/bash

# ETL Pipeline Docker Setup Script
# This script creates a Docker network and runs the ETL application with PostgreSQL database

set -e  # Exit on any error

# Configuration
NETWORK_NAME="etl-network"
DB_CONTAINER_NAME="etl-postgres-db"
APP_CONTAINER_NAME="etl-app"
POSTGRES_PASSWORD="admin123"
DB_IMAGE_NAME="etl-postgres"
APP_IMAGE_NAME="etl-app-image"

echo "=========================================="
echo "Starting ETL Pipeline Docker Setup"
echo "=========================================="

# Function to cleanup existing resources
cleanup() {
    echo "Cleaning up existing containers and network..."
    
    # Stop and remove containers if they exist
    docker stop $DB_CONTAINER_NAME 2>/dev/null || true
    docker stop $APP_CONTAINER_NAME 2>/dev/null || true
    docker rm $DB_CONTAINER_NAME 2>/dev/null || true
    docker rm $APP_CONTAINER_NAME 2>/dev/null || true
    
    # Remove network if it exists
    docker network rm $NETWORK_NAME 2>/dev/null || true
    
    echo "Cleanup completed."
}

# Function to create Docker network
create_network() {
    echo "Creating Docker network: $NETWORK_NAME"
    docker network create $NETWORK_NAME
    echo "Network created successfully."
}

# Function to build Docker images
build_images() {
    echo "Building Docker images..."
    
    # Build PostgreSQL database image
    echo "Building PostgreSQL database image..."
    cd db/
    docker build -t $DB_IMAGE_NAME .
    cd ../
    
    # Build ETL application image
    echo "Building ETL application image..."
    cd app/
    docker build -t $APP_IMAGE_NAME .
    cd ../
    
    echo "Docker images built successfully."
}

# Function to run PostgreSQL container
run_database() {
    echo "Starting PostgreSQL database container..."
    docker run -d \
        --name $DB_CONTAINER_NAME \
        --network $NETWORK_NAME \
        -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD \
        -p 5432:5432 \
        $DB_IMAGE_NAME
    
    echo "PostgreSQL container started successfully."
    echo "Waiting for database to be ready..."
    sleep 10  # Give the database time to initialize
}

# Function to run ETL application container
run_etl_app() {
    echo "Starting ETL application container..."
    docker run -d \
        --name $APP_CONTAINER_NAME \
        --network $NETWORK_NAME \
        -e DB_HOST=$DB_CONTAINER_NAME \
        -e DB_USER=admin \
        -e DB_PASSWORD=$POSTGRES_PASSWORD \
        -e DB_NAME=app_db \
        $APP_IMAGE_NAME
    
    echo "ETL application container started successfully."
}

# Function to show container status
show_status() {
    echo "=========================================="
    echo "Container Status:"
    echo "=========================================="
    docker ps --filter "network=$NETWORK_NAME" --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"
    
    echo ""
    echo "Network Information:"
    docker network inspect $NETWORK_NAME --format "{{.Name}}: {{range .Containers}}{{.Name}} {{end}}"
}

# Function to show logs
show_logs() {
    echo "=========================================="
    echo "Recent Application Logs:"
    echo "=========================================="
    docker logs --tail 20 $APP_CONTAINER_NAME 2>/dev/null || echo "No logs available yet."
}

# Main execution
main() {
    echo "Starting ETL Pipeline setup..."
    
    # Cleanup any existing resources
    cleanup
    
    # Create Docker network
    create_network
    
    # Build Docker images
    build_images
    
    # Run database container
    run_database
    
    # Run ETL application container
    run_etl_app
    
    # Show status
    show_status
    
    # Show logs
    show_logs
    
    echo ""
    echo "=========================================="
    echo "ETL Pipeline Setup Complete!"
    echo "=========================================="
    echo "Database available at: localhost:5432"
    echo "Database name: app_db"
    echo "Username: admin"
    echo ""
    echo "To view logs: docker logs $APP_CONTAINER_NAME"
    echo "To stop containers: docker stop $DB_CONTAINER_NAME $APP_CONTAINER_NAME"
    echo "To remove containers: docker rm $DB_CONTAINER_NAME $APP_CONTAINER_NAME"
    echo "To remove network: docker network rm $NETWORK_NAME"
}

# Handle script interruption
trap cleanup EXIT

# Run main function
main
