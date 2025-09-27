FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY . .

# Create directories for data and logs
RUN mkdir -p /app/loadingcsv /app/output /app/logs

# Expose port for the API
EXPOSE 5000

# Set default command to run the API service
CMD ["python", "csv_worker_api.py", "--host", "0.0.0.0", "--port", "5010"]
