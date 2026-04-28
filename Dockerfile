FROM python:3.11-slim

# Install system dependencies + tzdata for timezone support
RUN apt-get update && apt-get install -y \
    build-essential \
    libxml2-dev \
    libxslt-dev \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements from the backend folder into the context
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the backend source code into the current WORKDIR (/app)
COPY backend/ .

# Set timezone to America/New_York for scheduler
ENV TZ=America/New_York
ENV PYTHONUNBUFFERED=1

# Start the uvicorn server using the port variable
CMD ["sh", "-c", "uvicorn server:app --host 0.0.0.0 --port ${PORT}"]
