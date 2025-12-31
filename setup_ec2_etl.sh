#!/bin/bash
set -euo pipefail

echo "Starting EC2 ETL full setup..."

# 1. Update system packages only (no upgrade to avoid incompatibility)
echo " Updating system packages..."
sudo apt update -y

# 2. Install required packages
echo "Installing git, docker, docker-compose, python3, pip..."
sudo apt install -y git docker.io docker-compose docker-compose-plugin python3 python3-pip

# 3. Enable and start Docker
echo "Enabling and starting Docker..."
sudo systemctl enable docker
sudo systemctl start docker

# 4. Add 'ubuntu' user to docker group
echo "👤 Adding 'ubuntu' user to docker group..."
sudo usermod -aG docker $USER
echo " Docker group updated. Docker commands will require 'sudo' for this session."

# 5️⃣ Clone Git repository
REPO_URL="https://github.com/vieuxcolon/lancelot-sang-louis-dataeng.git"    # <-- Replace with your Git repo URL
REPO_DIR="${HOME}/$(basename $REPO_URL .git)"
if [ -d "$REPO_DIR" ]; then
    echo " Repository already exists. Pulling latest changes..."
    cd "$REPO_DIR"
    git pull
else
    echo "Cloning repository..."
    git clone "$REPO_URL"
    cd "$REPO_DIR"
fi

# 6. Copy .env file (if exists)
if [ -f ".env.example" ]; then
    echo " Setting up .env file..."
    cp .env.example .env
    echo " .env file created. Review and edit if necessary."
else
    echo " No .env.example found. Please copy your .env manually."
fi

# 7. Launch Docker Compose for ETL stack using sudo to avoid permission issues
echo "Launching Docker containers..."
sudo docker-compose down || true       # Stop any existing containers
sudo docker-compose pull                # Pull latest images
sudo docker-compose up -d               # Launch containers detached

# 8. Wait a few seconds for containers to initialize
echo " Waiting 15 seconds for containers to initialize..."
sleep 15

# 9. Verify running containers
echo " Listing running Docker containers:"
sudo docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo " EC2 ETL setup completed successfully!"
echo "Next steps:"
echo "1. Access Airflow UI: http://34.249.33.113:8080 (use .env credentials)"
echo "2. Access Mongo Express: http://34.249.33.113:8085"
echo "3. Access PGAdmin: http://34.249.33.113:5050"
echo "4. Connect to Postgres from your scripts using .env credentials"
