FROM python:3.10-slim

ARG BUILD_DATE
ARG BUILD_VERSION


LABEL maintainer="DalgoT4D"
LABEL org.opencontainers.image.source="https://github.com/DalgoT4D/prefect-proxy"
LABEL org.opencontainers.image.licenses="https://github.com/DalgoT4D/prefect-proxy?tab=AGPL-3.0-1-ov-file"
LABEL org.opencontainers.image.version=${BUILD_VERSION}
LABEL org.opencontainers.image.created=${BUILD_DATE}


ENV PIP_DEFAULT_TIMEOUT=100 \
    # Allow statements and log messages to immediately appear
    PYTHONUNBUFFERED=1 \
    # disable a pip version check to reduce run-time & log-spam
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # cache is useless in docker image, so disable to reduce image size
    PIP_NO_CACHE_DIR=1



# Set the working directory in the container
WORKDIR /app

COPY ./requirements.txt /app/

RUN set -ex \
    # Create a non-root user
    && addgroup --system --gid 1001 appgroup \
    && adduser --system --uid 1001 --gid 1001 --no-create-home appuser \
    # Upgrade the package index and install security upgrades
    && apt-get update \
    && apt-get upgrade -y \
    # Install dependencies
    && pip install -r requirements.txt \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*




COPY ./proxy /app/proxy

# Expose port 4300 to allow external access
EXPOSE 4300

CMD ["gunicorn", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "proxy.main:app", "--bind", "0.0.0.0:4300"]

# Set the user to run the application
USER appuser