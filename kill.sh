#!/bin/sh

#CELERY_WORKER_PID=`cat celeryworker.pid`
#kill -9 "${CELERY_WORKER_PID}"

ps aux | grep "prefect-proxy" | grep uvicorn | grep -v grep | awk '{print $2}' | xargs kill
