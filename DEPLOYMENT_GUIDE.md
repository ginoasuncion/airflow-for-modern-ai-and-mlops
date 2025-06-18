# 🚀 Airflow VM Deployment Guide

This guide will help you deploy Apache Airflow on a virtual machine (VM) for your web scraping DAG.

## 📋 Prerequisites

- A VM with Ubuntu 20.04+ or CentOS 7+
- At least 4GB RAM and 2 CPU cores
- SSH access to the VM
- Basic knowledge of Linux commands

## 🛠️ Step 1: VM Setup

### Option A: Google Cloud Platform (GCP)

```bash
# Create a VM instance
gcloud compute instances create airflow-vm \
  --zone=us-central1-a \
  --machine-type=e2-standard-2 \
  --image-family=ubuntu-2004-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB \
  --tags=http-server,https-server

# Connect to your VM
gcloud compute ssh airflow-vm --zone=us-central1-a
```

### Option B: AWS EC2

```bash
# Create EC2 instance with Ubuntu 20.04
# Instance type: t3.medium or larger
# Security group: Allow SSH (port 22) and HTTP (port 8080)
```

### Option C: Azure VM

```bash
# Create VM with Ubuntu 20.04
# Size: Standard_B2s or larger
# Network: Allow SSH and HTTP ports
```

## 🔧 Step 2: Install Dependencies

Once connected to your VM, run these commands:

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and pip
sudo apt install -y python3 python3-pip python3-venv

# Install system dependencies
sudo apt install -y build-essential libssl-dev libffi-dev python3-dev

# Install Docker (for easier deployment)
curl -fsSL https://get.docker.com -o get-docker.sh
sudo sh get-docker.sh
sudo usermod -aG docker $USER

# Install Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.20.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Logout and login again for Docker group to take effect
exit
# SSH back into your VM
```

## 🐳 Step 3: Deploy Airflow with Docker

```bash
# Clone this repository
git clone https://github.com/mlrepa/airflow-for-modern-ai-and-mlops.git
cd airflow-for-modern-ai-and-mlops

# Set the Airflow user ID
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Initialize the database
docker compose up airflow-init

# Start Airflow
docker compose up -d

# Check if services are running
docker ps
```

## 🌐 Step 4: Access Airflow Web UI

### For GCP:
```bash
# Create firewall rule to allow HTTP traffic
gcloud compute firewall-rules create allow-airflow \
  --allow tcp:8080 \
  --source-ranges 0.0.0.0/0 \
  --description "Allow Airflow web UI"

# Get your VM's external IP
gcloud compute instances describe airflow-vm --zone=us-central1-a --format="get(networkInterfaces[0].accessConfigs[0].natIP)"
```

### For AWS:
- Go to EC2 Console → Security Groups
- Add inbound rule: HTTP (port 8080) from anywhere (0.0.0.0/0)

### For Azure:
- Go to VM → Networking
- Add inbound port rule: HTTP (port 8080)

### Access the Web UI:
- Open your browser and go to: `http://YOUR_VM_IP:8080`
- Login with: `airflow` / `airflow`

## 📁 Step 5: Upload Your DAGs

### Option A: Using SCP (from your local machine)
```bash
# Upload DAG files to VM
scp -i your-key.pem dags/simple_web_scraper_dag.py ubuntu@YOUR_VM_IP:~/airflow-for-modern-ai-and-mlops/dags/

# Upload requirements
scp -i your-key.pem pyproject.toml ubuntu@YOUR_VM_IP:~/airflow-for-modern-ai-and-mlops/
```

### Option B: Using Git (on VM)
```bash
# On your VM, pull the latest changes
cd airflow-for-modern-ai-and-mlops
git pull origin main
```

## 🔄 Step 6: Restart Airflow to Load New DAGs

```bash
# Restart Airflow services
docker compose down
docker compose up -d

# Check DAGs are loaded
docker compose exec airflow-scheduler airflow dags list
```

## 🧪 Step 7: Test Your DAG

1. Go to Airflow Web UI: `http://YOUR_VM_IP:8080`
2. Find your DAG: `simple_web_scraper`
3. Click on the DAG name
4. Click "Trigger DAG" to run it manually
5. Monitor the execution in the "Graph" view

## 📊 Step 8: Monitor and Debug

### Check logs:
```bash
# View scheduler logs
docker compose logs airflow-scheduler

# View webserver logs
docker compose logs airflow-webserver

# View specific task logs
docker compose exec airflow-scheduler airflow tasks logs simple_web_scraper scrape_quotes latest
```

### Check DAG status:
```bash
# List all DAGs
docker compose exec airflow-scheduler airflow dags list

# Check DAG state
docker compose exec airflow-scheduler airflow dags state simple_web_scraper latest
```

## 🔧 Step 9: Production Considerations

### Security:
```bash
# Change default passwords
docker compose exec airflow-webserver airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password your-secure-password

# Remove default airflow user
docker compose exec airflow-webserver airflow users delete airflow
```

### Persistence:
```bash
# Create persistent volumes for logs and data
mkdir -p ~/airflow-logs ~/airflow-data

# Update docker-compose.yaml to use persistent volumes
# (Already configured in the provided docker-compose.yaml)
```

### Monitoring:
```bash
# Install monitoring tools
sudo apt install -y htop iotop

# Monitor system resources
htop
```

## 🚨 Troubleshooting

### Common Issues:

1. **DAG not appearing:**
   ```bash
   # Check DAG folder permissions
   ls -la dags/
   
   # Restart scheduler
   docker compose restart airflow-scheduler
   ```

2. **Import errors:**
   ```bash
   # Install missing packages
   docker compose exec airflow-scheduler pip install requests beautifulsoup4 pandas
   ```

3. **Permission issues:**
   ```bash
   # Fix file permissions
   sudo chown -R $USER:$USER dags/
   chmod -R 755 dags/
   ```

4. **Port already in use:**
   ```bash
   # Check what's using port 8080
   sudo netstat -tlnp | grep :8080
   
   # Kill the process or change Airflow port
   ```

### Get help:
```bash
# Check Airflow version
docker compose exec airflow-scheduler airflow version

# Check configuration
docker compose exec airflow-scheduler airflow config list
```

## 📈 Next Steps

1. **Scale up:** Add more workers for parallel processing
2. **Add monitoring:** Set up Prometheus/Grafana
3. **Backup:** Configure database backups
4. **CI/CD:** Set up automated DAG deployment
5. **Security:** Configure SSL certificates
6. **Load balancing:** Set up reverse proxy (nginx)

## 🔗 Useful Commands

```bash
# Stop Airflow
docker compose down

# Start Airflow
docker compose up -d

# View all logs
docker compose logs -f

# Execute commands in Airflow container
docker compose exec airflow-scheduler airflow [command]

# Backup database
docker compose exec postgres pg_dump -U airflow airflow > backup.sql

# Restore database
docker compose exec -T postgres psql -U airflow airflow < backup.sql
```

Your Airflow instance is now ready to run your web scraping DAG! 🎉 