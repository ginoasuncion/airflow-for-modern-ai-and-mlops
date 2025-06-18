#!/bin/bash

# Airflow VM Setup Script
# This script automates the installation of Apache Airflow on a VM

set -e  # Exit on any error

echo "🚀 Starting Airflow VM Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   print_error "This script should not be run as root"
   exit 1
fi

# Update system
print_status "Updating system packages..."
sudo apt update && sudo apt upgrade -y

# Install Python and dependencies
print_status "Installing Python and system dependencies..."
sudo apt install -y python3 python3-pip python3-venv build-essential libssl-dev libffi-dev python3-dev curl wget git

# Install Docker
print_status "Installing Docker..."
if ! command -v docker &> /dev/null; then
    curl -fsSL https://get.docker.com -o get-docker.sh
    sudo sh get-docker.sh
    sudo usermod -aG docker $USER
    print_warning "Docker installed. You may need to logout and login again for group changes to take effect."
else
    print_status "Docker is already installed"
fi

# Install Docker Compose
print_status "Installing Docker Compose..."
if ! command -v docker-compose &> /dev/null; then
    sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
else
    print_status "Docker Compose is already installed"
fi

# Create Airflow directory
AIRFLOW_DIR="$HOME/airflow-for-modern-ai-and-mlops"
print_status "Setting up Airflow directory at $AIRFLOW_DIR"

if [ ! -d "$AIRFLOW_DIR" ]; then
    git clone https://github.com/mlrepa/airflow-for-modern-ai-and-mlops.git "$AIRFLOW_DIR"
else
    print_status "Airflow directory already exists, updating..."
    cd "$AIRFLOW_DIR"
    git pull origin main
fi

cd "$AIRFLOW_DIR"

# Create necessary directories
print_status "Creating necessary directories..."
mkdir -p dags logs plugins config

# Set Airflow user ID
print_status "Setting Airflow user ID..."
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Set proper permissions
print_status "Setting proper permissions..."
chmod -R 755 dags/
chmod -R 755 logs/
chmod -R 755 plugins/
chmod -R 755 config/

# Initialize Airflow database
print_status "Initializing Airflow database..."
docker compose up airflow-init

# Start Airflow services
print_status "Starting Airflow services..."
docker compose up -d

# Wait for services to be ready
print_status "Waiting for services to be ready..."
sleep 30

# Check if services are running
print_status "Checking service status..."
if docker compose ps | grep -q "Up"; then
    print_status "✅ Airflow services are running successfully!"
else
    print_error "❌ Some services failed to start. Check logs with: docker compose logs"
    exit 1
fi

# Get VM IP address
VM_IP=$(curl -s ifconfig.me 2>/dev/null || curl -s ipinfo.io/ip 2>/dev/null || echo "YOUR_VM_IP")

# Display success message
echo ""
echo "🎉 Airflow setup completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Access Airflow Web UI: http://$VM_IP:8080"
echo "2. Login with: airflow / airflow"
echo "3. Your DAGs are in: $AIRFLOW_DIR/dags/"
echo ""
echo "🔧 Useful commands:"
echo "  - View logs: docker compose logs -f"
echo "  - Stop Airflow: docker compose down"
echo "  - Start Airflow: docker compose up -d"
echo "  - Check DAGs: docker compose exec airflow-scheduler airflow dags list"
echo ""
echo "📁 Your DAG files:"
ls -la dags/

# Check if firewall needs to be configured
print_warning "Don't forget to configure your cloud provider's firewall to allow port 8080!"

echo ""
print_status "Setup complete! 🚀" 