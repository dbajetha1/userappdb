# Use official Python image
FROM python:3.11-slim

# Set environment variables
ENV ENV=local

# Set working directory
WORKDIR /userappdb

# Install system dependencies (optional: for psycopg2, MySQL, etc.)
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install pip & upgrade
RUN pip install --upgrade pip

# Copy requirements first (for caching)
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt gunicorn

# Copy project files
COPY . .

# Expose port
EXPOSE 5000

# Default command
CMD ["gunicorn", "-b", "0.0.0.0:5000", "run:app"]