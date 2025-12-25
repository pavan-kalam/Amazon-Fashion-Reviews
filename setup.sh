#!/bin/bash

# Setup script for Airflow Reddit Data Pipeline

echo "========================================="
echo "Airflow Reddit Data Pipeline Setup"
echo "========================================="

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

# Check if Docker Compose is installed
if ! command -v docker compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Create necessary directories
echo "Creating project directories..."
mkdir -p dags logs plugins config scripts

# Set AIRFLOW_UID if not set (for Linux)
if [ -z "$AIRFLOW_UID" ]; then
    echo "Setting AIRFLOW_UID..."
    export AIRFLOW_UID=$(id -u)
    echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
fi

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
AIRFLOW_UID=$AIRFLOW_UID
AIRFLOW_PROJ_DIR=.
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# AWS Configuration
AWS_ACCESS_KEY_ID=your_aws_access_key_id
AWS_SECRET_ACCESS_KEY=your_aws_secret_access_key
AWS_DEFAULT_REGION=us-east-1

# Reddit API Configuration
REDDIT_CLIENT_ID=your_reddit_client_id
REDDIT_CLIENT_SECRET=your_reddit_client_secret
REDDIT_USER_AGENT=reddit_data_pipeline/1.0

# AWS Services Configuration
S3_BUCKET_NAME=reddit-data-pipeline
GLUE_DATABASE_NAME=reddit_database
ATHENA_DATABASE_NAME=reddit_database
ATHENA_S3_OUTPUT_LOCATION=s3://reddit-data-pipeline/athena-results/
REDSHIFT_CLUSTER_IDENTIFIER=reddit-cluster
REDSHIFT_DATABASE=reddit_warehouse
REDSHIFT_USER=redshift_user
REDSHIFT_PASSWORD=redshift_password
REDSHIFT_HOST=your-redshift-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
EOF
    echo ".env file created. Please update it with your credentials."
fi

echo ""
echo "========================================="
echo "Setup complete!"
echo "========================================="
echo ""
echo "Next steps:"
echo "1. Update .env file with your AWS and Reddit API credentials"
echo "2. Run: docker compose up -d"
echo "3. Access Airflow UI at: http://localhost:8080"
echo "   Username: airflow"
echo "   Password: airflow"
echo "4. Access pgAdmin at: http://localhost:5050"
echo "   Email: admin@admin.com"
echo "   Password: admin"
echo ""
echo "To connect pgAdmin to PostgreSQL:"
echo "  Host: postgres (or localhost if connecting from host)"
echo "  Port: 5432"
echo "  Database: airflow"
echo "  Username: airflow"
echo "  Password: airflow"
echo ""

