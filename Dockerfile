# SetStream Dockerfile
# Multi-stage build for R-based data pipeline

FROM rocker/r-ver:4.3.2 AS base

# Install system dependencies
RUN apt-get update && apt-get install -y \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libpq-dev \
    libsodium-dev \
    git \
    make \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy renv files first for caching
COPY renv.lock .Rprofile ./
COPY renv/activate.R renv/

# Install renv and restore packages
RUN R -e "install.packages('renv', repos='https://cloud.r-project.org')"
RUN R -e "renv::restore()"

# Copy application code
COPY . .

# Create required directories
RUN mkdir -p data/lake data/warehouse data/state logs

# Production stage
FROM base AS production

# Set environment
ENV R_CONFIG_ACTIVE=default
ENV MAKEFLAGS="-s"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD R -e "DBI::dbConnect(duckdb::duckdb(), dbdir='data/warehouse/setstream.duckdb', read_only=TRUE); DBI::dbDisconnect(conn)" || exit 1

# Default command: run pipeline
CMD ["Rscript", "scripts/run_pipeline.R"]

# API stage
FROM base AS api

EXPOSE 8000

ENV R_CONFIG_ACTIVE=default

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

CMD ["Rscript", "scripts/run_api.R"]

# Dashboard stage
FROM base AS dashboard

EXPOSE 3838

ENV R_CONFIG_ACTIVE=default

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:3838 || exit 1

CMD ["Rscript", "scripts/run_dashboard.R"]
