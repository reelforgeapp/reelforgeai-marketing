# =============================================================================
# ReelForge Marketing Engine - Dockerfile
# Multi-process container: FastAPI + Celery Worker + Celery Beat
# =============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium --with-deps

# Download spaCy model (optional - for NLP scoring)
RUN python -m spacy download en_core_web_sm || true

# Copy application code
COPY . .

# Create startup script
RUN echo '#!/bin/bash\n\
set -e\n\
\n\
echo "Starting ReelForge Marketing Engine..."\n\
\n\
# Start Celery worker in background\n\
celery -A celery_config worker --loglevel=info --concurrency=2 &\n\
\n\
# Start Celery beat scheduler in background\n\
celery -A celery_config beat --loglevel=info &\n\
\n\
# Start FastAPI server (foreground)\n\
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}\n\
' > /app/start.sh && chmod +x /app/start.sh

# Expose port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT:-8080}/health || exit 1

# Run startup script
CMD ["/app/start.sh"]
