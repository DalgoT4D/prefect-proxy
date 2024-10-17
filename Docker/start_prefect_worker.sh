#!/bin/sh

# Wait to ensure the server and work pool are initialized
sleep 15

# Start the worker and connect it to the work pool
prefect worker start --pool "$WORK_POOL_NAME" --work-queue "$WORK_QUEUE_NAME" --limit "$FLOW_RUN_LIMIT" --name "$WORKER_NAME" 
