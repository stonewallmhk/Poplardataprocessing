# Use a slim Python 3.11 image — matches the version we developed against
FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install dependencies first (Docker layer cache — only rebuilds if requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# Cloud Run Jobs expect the container to run and exit when done
CMD ["python", "main.py"]
