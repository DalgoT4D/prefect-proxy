"""script to set project_dir for a deployment / all deployments"""

import sys
import argparse
import requests

BASE_URL = "http://localhost:4200/api"

parser = argparse.ArgumentParser(usage="sets work queue for deployments")
parser.add_argument("--show-and-exit", action="store_true")
parser.add_argument("--deployment-id", required=True)
parser.add_argument("--work-queue-name")
parser.add_argument("--work-pool-name")
args = parser.parse_args()

deployment_id = args.deployment_id


def get_deployment(p_deployment_id: str) -> dict:
    """fetch the deployment with the given id"""
    r = requests.get(f"{BASE_URL}/deployments/{p_deployment_id}", timeout=20)
    try:
        r.raise_for_status()
    except Exception:
        print(f"no deployment found with id {p_deployment_id}")
        sys.exit(1)
    return r.json()


deployment = get_deployment(deployment_id)

old_work_queue_name = deployment["work_queue_name"]
old_work_pool_name = deployment["work_pool_name"]

if args.show_and_exit:
    print(f'work queue "{old_work_queue_name}" for deployment {deployment_id}')
    print(f'work pool "{old_work_pool_name}" for deployment {deployment_id}')
    sys.exit(0)

new_work_queue_name = args.work_queue_name or old_work_queue_name
new_work_pool_name = args.work_pool_name or old_work_pool_name

if old_work_queue_name == new_work_queue_name and old_work_pool_name == new_work_pool_name:
    print(f'work queue "{old_work_queue_name}" already set for deployment {deployment_id}')
    print(f'work pool "{old_work_pool_name}" already set for deployment {deployment_id}')
    sys.exit(0)

update_payload = {"work_queue_name": new_work_queue_name, "work_pool_name": new_work_pool_name}

print(
    f"updating deployment {deployment_id} "
    + f"work_queue_name={new_work_queue_name} "
    + f"work_pool_name={new_work_pool_name}"
)
response = requests.patch(
    f"{BASE_URL}/deployments/{deployment_id}",
    json=update_payload,
    timeout=20,
)
try:
    response.raise_for_status()
except Exception:
    print(response.json())
