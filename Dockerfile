# 1. Base Image
FROM python:3.12-slim

# 2. Set working directory
WORKDIR /app

# 3. Copy project files
COPY . /app

# 4. Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# 5. Expose Flask port
EXPOSE 5000

# 6. Start the app with gunicorn
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "app:app"]