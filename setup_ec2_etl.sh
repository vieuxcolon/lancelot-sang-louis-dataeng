#!/bin/bash
set -euo pipefail

echo " Starting EC2 ETL full setup..."

# 1. Update & upgrade system
echo "🛠 Updating system packages..."
sudo apt update && sudo apt upgrade -y

# 2. Install required packages
echo " Installing git, docker, docker-compose, python3, pip..."
sudo apt install -y git docker.io docker-compose python3 python3-pip

# 3. Enable and start Docker
echo " Enabling and starting Docker..."
sudo systemctl enable docker
sudo systemctl start docker

# 4. Add ubuntu user to docker group
echo " Adding 'ubuntu' user to docker group..."
sudo usermod -aG docker $USER
# Activate docker group without logout
newgrp docker <<EONG
echo " Docker group updated for current session."
EONG

# 5. Clone Git repository
REPO_URL="https://github.com/vieuxcolon/lancelot-sang-louis-dataeng.git"   # <-- Replace with your Git repo URL
REPO_DIR="${HOME}/$(basename $REPO_URL .git)"
if [ -d "$REPO_DIR" ]; then
    echo " Repo already exists. Pulling latest changes..."
    cd "$REPO_DIR"
    git pull
else
    echo " Cloning repository..."
    git clone "$REPO_URL"
    cd "$REPO_DIR"
fi

# 6. Copy .env file (if exists)
if [ -f ".env.example" ]; then
    echo "⚙ Setting up .env file..."
    cp .env.example .env
    echo " .env file created. Review and edit if necessary."
else
    echo "⚠ No .env.example found. Please copy your .env manually."
fi

# 7. Launch Docker Compose for ETL stack
echo " Launching Docker containers..."
docker-compose down || true  # Stop any existing containers
docker-compose pull          # Pull latest images
docker-compose up -d         # Launch containers in detached mode

# 8. Wait a few seconds for containers to initialize
echo " Waiting 15 seconds for containers to initialize..."
sleep 15

# 9. Verify running containers
echo " Listing running Docker containers:"
docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo " EC2 ETL full setup completed successfully!"
echo "Next steps:"
echo "1. Access Airflow UI: http://54.171.161.175:8080 (use .env credentials)"
echo "2. Access Mongo Express: http://54.171.161.175:8085"
echo "3. Access PGAdmin: http://54.171.161.175:5050"
echo "4. Connect to Postgres from your scripts using .env credentials"
