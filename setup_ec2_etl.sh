#!/bin/bash
set -euo pipefail

echo "Starting EC2 ETL full setup..."

# --------------------------------------------------
# 1. Update package index (no upgrade)
# --------------------------------------------------
echo " Updating system packages..."
sudo apt update -y

# --------------------------------------------------
# 2. Install base dependencies
# --------------------------------------------------
echo " Installing base dependencies..."
sudo apt install -y \
  git \
  ca-certificates \
  curl \
  gnupg \
  lsb-release \
  python3 \
  python3-pip

# --------------------------------------------------
# 3. Install Docker + Docker Compose v2 (official repo)
# --------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
  echo " Installing Docker from official repository..."

  sudo mkdir -p /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

  echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

  sudo apt update -y
  sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
else
  echo " Docker already installed. Skipping Docker installation."
fi

# --------------------------------------------------
# 4. Enable and start Docker
# --------------------------------------------------
echo " Enabling and starting Docker..."
sudo systemctl enable docker
sudo systemctl start docker

# --------------------------------------------------
# 5. Add current user to docker group (non-destructive)
# --------------------------------------------------
echo " Adding user to docker group..."
sudo usermod -aG docker "$USER"
echo " NOTE: You may need to log out and log back in for group changes to apply."

# --------------------------------------------------
# 6. Clone or update Git repository
# --------------------------------------------------
REPO_URL="https://github.com/vieuxcolon/lancelot-sang-louis-dataeng.git"
REPO_DIR="${HOME}/$(basename "$REPO_URL" .git)"

if [ -d "$REPO_DIR/.git" ]; then
  echo " Repository already exists. Pulling latest changes..."
  cd "$REPO_DIR"
  git pull
else
  echo " Cloning repository..."
  git clone "$REPO_URL"
  cd "$REPO_DIR"
fi

# --------------------------------------------------
# 7. Setup .env file (NO overwrite)
# --------------------------------------------------
if [ -f ".env.example" ] && [ ! -f ".env" ]; then
  echo " Creating .env file from .env.example..."
  cp .env.example .env
  echo " .env file created. Review and edit if necessary."
elif [ -f ".env" ]; then
  echo " .env already exists. Skipping overwrite."
else
  echo " No .env.example found. Please provide .env manually."
fi

# --------------------------------------------------
# 8. Launch Docker Compose stack (Compose v2)
# --------------------------------------------------
echo " Launching Docker containers..."
sudo docker compose down || true
sudo docker compose pull
sudo docker compose up -d

# --------------------------------------------------
# 9. Wait for containers to initialize
# --------------------------------------------------
echo " Waiting 15 seconds for containers to initialize..."
sleep 15

# --------------------------------------------------
# 10. Verify running containers
# --------------------------------------------------
echo " Running Docker containers:"
sudo docker ps --format "table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}"

echo " EC2 ETL setup completed successfully!"
echo
echo " Next steps:"
echo " 1. Access Airflow UI:       http://<EC2_PUBLIC_IP>:8080"
echo " 2. Access Mongo Express:    http://<EC2_PUBLIC_IP>:8085"
echo " 3. Access PGAdmin:          http://<EC2_PUBLIC_IP>:5050"
echo " 4. Use credentials defined in the .env file"
