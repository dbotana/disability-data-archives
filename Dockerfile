# =============================================================================
# Federal Disability Data Monitor — Dockerfile
# =============================================================================
# Uses Python 3.11-slim. NLP models are pre-downloaded during build to avoid
# slow startup and failures in air-gapped environments.
# =============================================================================

FROM python:3.11-slim

# System dependencies for lxml, aiosqlite, and sentence-transformers
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev \
    libxslt1-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python dependencies (torch is large — this layer is cached separately)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Pre-download the MiniLM embedding model during build
# (~80MB — avoids runtime download and enables air-gapped operation)
RUN python -c "\
from sentence_transformers import SentenceTransformer; \
SentenceTransformer('all-MiniLM-L6-v2', cache_folder='/app/models')" \
    || echo "Warning: Could not pre-download NLP model (network may be unavailable)"

# Copy application source
COPY *.py ./
COPY config.yaml ./
COPY templates/ ./templates/

# Create runtime directories
RUN mkdir -p data archive logs models data/digests

# Expose dashboard port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=60s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command
CMD ["python", "main.py", "--config", "config.yaml"]
