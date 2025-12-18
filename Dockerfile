FROM apache/airflow:3.1.0

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

