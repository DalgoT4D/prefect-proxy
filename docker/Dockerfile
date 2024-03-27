FROM python:3.10 as python-base
RUN mkdir proxy
WORKDIR  proxy
COPY ../../../requirements.txt ./requirements.txt
RUN pip install --no-cache-dir --upgrade -r requirements.txt
COPY ../../../proxy/* ./
CMD ["gunicorn", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "main:app", "--bind", "0.0.0.0:8080"]
