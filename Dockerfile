FROM python:3.10-slim as python-base
# Set the working directory in the container
WORKDIR /app

COPY ./requirements.txt /app/

RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./proxy /app/proxy

# Expose port 4300 to allow external access
EXPOSE 4300

CMD ["gunicorn", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "proxy.main:app", "--bind", "0.0.0.0:4300"]
