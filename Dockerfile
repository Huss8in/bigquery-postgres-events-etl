# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY flask_server.py .
COPY .env .

# Create directory for state persistence
RUN mkdir -p /app/data

# Expose Flask port
EXPOSE 5000

# Set environment variable for timestamp file location
ENV TIMESTAMP_FILE=/app/data/last_timestamp.txt

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:5000/health')"

# Run the Flask server
CMD ["python", "flask_server.py"]
