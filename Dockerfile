# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies (for psycopg2, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire application
COPY . .

# Expose the port your app runs on
EXPOSE 8000

# Command to run the application with uvicorn
# Use --reload only for development; for production remove --reload
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]