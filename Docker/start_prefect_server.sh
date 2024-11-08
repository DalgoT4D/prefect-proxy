#!/bin/sh

# Start Prefect server in the background
prefect server start &

# Wait for the server to start
sleep 10

# # Create the work pool
# prefect work-pool create --type "$WORK_POOL_TYPE" "$WORK_POOL_NAME"

# # Create the work queues
# prefect work-queue create --limit "$QUEUE1_CONCURRENCY_LIMIT" --pool "$WORK_POOL_NAME" "$PREFECT_WORK_QUEUE1"
# prefect work-queue create --limit "$QUEUE2_CONCURRENCY_LIMIT" --pool "$WORK_POOL_NAME" "$PREFECT_WORK_QUEUE2"

# Keep the container running
tail -f /dev/null
