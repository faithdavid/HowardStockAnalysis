FROM python:3.11-slim

# Install system dependencies if any are needed (e.g., lxml needs some libs)
RUN apt-get update && apt-get install -y \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source code
COPY . .

# Set environment variable for the port (Railway will override this)
ENV PORT=8000
ENV PYTHONUNBUFFERED=1

# Start the uvicorn server
# We use sh -c to allow environment variable expansion of $PORT
CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port ${PORT}"]
