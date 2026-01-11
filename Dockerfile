FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget curl gnupg ca-certificates \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libdbus-1-3 libxkbcommon0 libatspi2.0-0 libxcomposite1 libxdamage1 \
    libxfixes3 libxrandr2 libgbm1 libasound2 libpango-1.0-0 \
    libpangocairo-1.0-0 libcairo2 libx11-xcb1 libxcb1 \
    fonts-liberation fonts-noto-color-emoji \
    libglib2.0-0 libgtk-3-0 libx11-6 libxcb-shm0 libxext6 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright
RUN playwright install chromium

# Copy application code
COPY . .

# Create startup script
RUN echo '#!/bin/bash\n\
set -e\n\
echo "Starting ReelForge Marketing Engine v3..."\n\
celery -A celery_config worker --loglevel=info --concurrency=2 &\n\
celery -A celery_config beat --loglevel=info &\n\
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}\n\
' > /app/start.sh && chmod +x /app/start.sh

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

CMD ["/app/start.sh"]
