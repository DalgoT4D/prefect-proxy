#A multistage Dockerfile to create the prefect proxy image based on the Fast API code 
FROM python:3.10 as build
LABEL Stage="Build"


ENV PIP_DEFAULT_TIMEOUT=100 \
    # Allow statements and log messages to immediately appear
    PYTHONUNBUFFERED=1 \
    # disable a pip version check to reduce run-time & log-spam
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    # cache is useless in docker image, so disable to reduce image size
    PIP_NO_CACHE_DIR=1


# install uv. This makes installation of packages faster compared to pip
ADD https://astral.sh/uv/install.sh /install.sh


WORKDIR /app

COPY ../requirements.txt /app/

#Install system dependencies
RUN set -ex \
    && apt-get update \
    && apt-get upgrade -y \
    && chmod -R 755 /install.sh && /install.sh && rm /install.sh \
    && /root/.cargo/bin/uv pip install --system --upgrade pip \
    && /root/.cargo/bin/uv pip install --system --no-cache-dir -r requirements.txt \
    # Clean up
    && apt-get autoremove -y \
    && apt-get clean -y \
    && rm -rf /var/lib/apt/lists/*

# Copy the proxy directory contents into the container at /app/proxy
COPY ../proxy /app/proxy

RUN ls -la /app

# Stage 2 - Runtime
from python:3.10-slim-bookworm as runtime

ARG BUILD_DATE
ARG BUILD_VERSION

LABEL maintainer="DalgoT4D"
LABEL org.opencontainers.image.source="https://github.com/DalgoT4D/prefect-proxy"
LABEL org.opencontainers.image.licenses="https://github.com/DalgoT4D/prefect-proxy?tab=AGPL-3.0-1-ov-file"
LABEL org.opencontainers.image.version=${BUILD_VERSION}
LABEL org.opencontainers.image.created=${BUILD_DATE}


RUN useradd -m app_user \
    && mkdir /app \
    && chown -R app_user:app_user /app
USER app_user
WORKDIR /app


COPY --from=build --chown=app_user:app_user /usr/local/lib/python3.10/site-packages /usr/local/lib/python3.10/site-packages
# This is added to get packages installed in the build stage
COPY --from=build --chown=app_user /usr/local/ /usr/local/
COPY --from=build --chown=app_user:app_user ${WORKDIR}/app ${WORKDIR}

WORKDIR /app

# Expose port 8080 to allow external access
EXPOSE 8080
CMD ["gunicorn", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "proxy.main:app", "--bind", "0.0.0.0:8080"]
