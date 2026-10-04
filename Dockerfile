# syntax=docker/dockerfile:1.7
# ---------- build stage: compile wheels into an isolated venv ----------
FROM python:3.12-slim-bookworm AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PYTHONDONTWRITEBYTECODE=1
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
WORKDIR /build
COPY requirements.txt requirements.lock* ./
RUN if [ -f requirements.lock ]; then \
        pip install --require-hashes -r requirements.lock; \
    else \
        echo "WARNING: requirements.lock missing - installing unpinned ranges (not for production)"; \
        pip install -r requirements.txt; \
    fi

# ---------- runtime stage: minimal, non-root ----------
FROM python:3.12-slim-bookworm AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PATH="/opt/venv/bin:$PATH"
RUN groupadd --gid 10001 app && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin app \
    && apt-get update && apt-get upgrade -y && rm -rf /var/lib/apt/lists/*
COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY --chown=10001:10001 src/ ./src/
USER 10001:10001
EXPOSE 8000 8081
# Exec form so SIGTERM reaches Python directly (graceful Kafka shutdown).
ENTRYPOINT ["python", "-m", "src.main"]
