# 1. Base Image
FROM python:3.12-slim

# 2. Set working directory inside container
WORKDIR /app

# 3. Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# 4. Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copy source code
COPY . .

# 6. Expose Flask and Dash ports
EXPOSE 5000
EXPOSE 8050

# 7. Start Flask using Gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]
