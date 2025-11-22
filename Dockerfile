FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    fontconfig \
    libfreetype6 \
    libpng16-16 \
    libjpeg62-turbo \
    libx11-6 \
    libxext6 \
    libxrender1 \
    libxft2 \
    libssl-dev \
    libffi-dev \
    xz-utils \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install wkhtmltopdf (Ubuntu focal build works for Debian)
RUN wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.focal_amd64.deb \
    && dpkg -i wkhtmltox_0.12.6-1.focal_amd64.deb || apt-get -f install -y \
    && rm wkhtmltox_0.12.6-1.focal_amd64.deb

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

EXPOSE 5000
EXPOSE 8050

CMD ["gunicorn", "app:app", "-w", "4", "-k", "gthread", "--threads", "2", "--bind", "0.0.0.0:5000", "--preload"]

