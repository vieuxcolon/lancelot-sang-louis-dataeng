#!/bin/bash
set -euo pipefail

echo "Starting EC2 ETL full setup..."

# 1. Update system packages only (no upgrade to avoid incompatibility)
echo " Updating system packages..."
sudo apt update -y

# 2. Install required packages (NO docker-compose v1)
echo " Installing git, docker, docker compose plugin, python3, pip..."
sudo apt install -y \
  git \
  docker.io \
  docker-compose-plugin \
  python3 \
  python3-pip

# 3. Enable and start Docker
echo " Enabling and starting Docker..."
sudo systemctl enable docker
sudo systemctl start docker

# 4. Add current user to docker group
echo " Adding user to docker group..."
sudo usermod -aG docker "$USER"
echo " Docker group updated. You may need to log out/in for it to take effect."

# 5. Clone Git repository
REPO_URL="https://github.com/vieuxcolon/lancelot-sang-louis-dataeng.git"
REPO_DIR="${HOME}/$(basename "$REPO_URL" .git)"

if [ -d "$REPO_DIR" ]; then
    echo " Repository already exists. Pulling latest changes..."
    cd "$REPO_DIR"
    git pull
else
    echo " Cloning repository..."
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

# 7. Launch Docker Compose for ETL stack (Compose v2 syntax)
echo " Launching Docker containers..."
sudo docker compose down || true
sudo docker compose pull
sudo docker compose up -d

# 8. Wait for containers to initialize
echo " Waiting 15 seconds for containers to initialize..."
sleep 15

# 9. Verify running containers
echo " Listing running Docker containers:"
sudo docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo " EC2 ETL setup completed successfully!"
echo " Next steps:"
echo " 1. Access Airflow UI: http://<EC2_PUBLIC_IP>:8080"
echo " 2. Access Mongo Express: http://<EC2_PUBLIC_IP>:8085"
echo " 3. Access PGAdmin: http://<EC2_PUBLIC_IP>:5050"
echo " 4. Use .env credentials for database connections"
