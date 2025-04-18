"""
this script creates a custom webhook block document and a webhook notification policy
this would be used to notify about dalgo backend on various flow run events
"""

import os
import requests
import prefect
from dotenv import load_dotenv


def create_notification(
    prefect_api_url: str, django_webhook_url: str, django_webhook_api_key: str, is_active: bool
):
    """main function"""
    try:
        print(f"Prefect version: {prefect.__version__}")

        # get custom webhook block type
        payload = {
            "block_schemas": {
                "block_capabilities": {"all_": ["notify"]},
            },
        }
        res = requests.post(f"{prefect_api_url}/block_types/filter", json=payload, timeout=15)
        res.raise_for_status()

        custom_webhook_type = [
            block_type for block_type in res.json() if block_type["slug"] == "custom-webhook"
        ]
        if not custom_webhook_type:
            print("No block types with slug 'custom-webhook' found.")
            return

        print(f"Found block type with slug 'custom-webhook' and id: {custom_webhook_type[0]['id']}")

        # get schema for the block
        payload = {
            "block_schemas": {
                "block_type_id": {"any_": [custom_webhook_type[0]["id"]]},
                "version": {"any_": [prefect.__version__]},
            },
        }
        res = requests.post(f"{prefect_api_url}/block_schemas/filter", json=payload, timeout=15)
        res.raise_for_status()

        custom_webhook_block_schema = [
            block_schema
            for block_schema in res.json()
            if block_schema["version"] == prefect.__version__
        ]
        if not custom_webhook_block_schema:
            print(
                f"No block schema for block type 'custom-webhook' with version {prefect.__version__} found."
            )
            return

        print(
            f"Found block schema for block type 'custom-webhook' with version {prefect.__version__} "
            "and id : {custom_webhook_block_schema[0]['id']}"
        )

        # create the custom webhook block document
        payload = {
            "block_schema_id": custom_webhook_block_schema[0]["id"],
            "block_type_id": custom_webhook_type[0]["id"],
            "name": "dalgo-custom-webhook-block",
            "is_anonymous": False,
            "data": {
                "url": django_webhook_url,
                "name": "Dalgo webhook",
                "headers": {
                    "X-Notification-Key": django_webhook_api_key,
                },
                "json_data": {"body": "{{body}}"},
                "timeout": 30,
            },
        }
        res = requests.post(f"{prefect_api_url}/block_documents/", json=payload, timeout=15)
        res.raise_for_status()

        custom_webhook_block_document = res.json()
        print(
            f"Created custom webhook block document with id {custom_webhook_block_document['id']}"
        )

        # create the webhook notification policy
        payload = {
            "is_active": is_active,  # toggle this if you want an active one or you can do it from the UI
            "state_names": [
                "Completed",
                "Cancelled",
                "Crashed",
                "Failed",
                "TimedOut",
                "Pending",
                "Running",
            ],
            "tags": [],
            "block_document_id": custom_webhook_block_document["id"],
            "message_template": "Flow run {flow_run_name} with id {flow_run_id} "
            "entered state {flow_run_state_name}",
        }
        res = requests.post(
            f"{prefect_api_url}/flow_run_notification_policies/", json=payload, timeout=15
        )
        res.raise_for_status()

        webhook_notification_policy = res.json()
        print(f"Created webhook notification policy with id {webhook_notification_policy['id']}")
        print("Webhook is currenty active" if is_active else "Webhook is currently inactive")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        if "res" in locals():
            print(f"Response status code: {res.status_code}")
            print(f"Response content: {res.text}")
    except Exception as err:
        print(f"An error occurred: {err}")


if __name__ == "__main__":
    load_dotenv(".env.webhook_notification_policy")

    prefect_api_url_param = os.getenv("PREFECT_API_URL")
    django_webhook_url_param = os.getenv("DJANGO_WEBHOOK_URL")
    django_webhook_api_key_param = os.getenv("DJANGO_WEBHOOK_API_KEY")

    if prefect_api_url_param and django_webhook_url_param and django_webhook_api_key_param:
        create_notification(
            prefect_api_url_param, django_webhook_url_param, django_webhook_api_key_param, False
        )
    else:
        print(
            "Please set the environment variables PREFECT_API_URL, DJANGO_WEBHOOK_URL, "
            "and DJANGO_WEBHOOK_API_KEY in the .env.webhook_notification_policy file."
        )
