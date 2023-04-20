# prefect-proxy

Since Prefect exposes an async Python interface and Django does not play well with async functions, we split the Prefect interface off into a FastAPI project

These endpoints will be called only from the Django server or from testing scripts

pip install -r requirements.txt

uvicorn main:app --reload --port <port number>

make sure to add this port number into the .env for DDP_backend using the variable PREFECT_PROXY_API_URL