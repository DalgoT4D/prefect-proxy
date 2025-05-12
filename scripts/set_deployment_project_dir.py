"""script to set project_dir for a deployment / all deployments"""

import argparse
import requests

BASE_URL = "http://localhost:4200/api"

parser = argparse.ArgumentParser(usage="sets project_dir for deployments")
parser.add_argument("--show", action="store_true")
parser.add_argument("--deployment-id")
args = parser.parse_args()

if args.deployment_id:
    payload = {
        "deployments": {"operator": "and_", "id": {"any_": [args.deployment_id]}},
    }
else:
    payload = {}

r = requests.post(f"{BASE_URL}/deployments/filter", json=payload, timeout=20)
r.raise_for_status()
deployments = r.json()

if args.show:
    for deployment in deployments:
        if deployment.get("parameters"):
            if deployment["parameters"].get("config"):
                if deployment["parameters"]["config"].get("tasks"):
                    for task in deployment["parameters"]["config"]["tasks"]:
                        if (
                            (
                                task.get("project_dir")
                                and "/home/ddp/clientdbts" in task.get("project_dir")
                            )
                            or (
                                task.get("working_dir")
                                and "/home/ddp/clientdbts" in task.get("working_dir")
                            )
                            or (
                                task.get("profiles_dir")
                                and "/home/ddp/clientdbts" in task.get("profiles_dir")
                            )
                        ):
                            print(deployment["id"])
else:
    for deployment in deployments:
        deployment_id = deployment["id"]
        changed = False
        if deployment.get("parameters"):
            if deployment["parameters"].get("config"):
                if deployment["parameters"]["config"].get("tasks"):
                    for task in deployment["parameters"]["config"]["tasks"]:
                        if task.get("project_dir") and "/home/ddp/clientdbts" in task.get(
                            "project_dir"
                        ):
                            task["project_dir"] = task["project_dir"].replace(
                                "/home/ddp/clientdbts", "/mnt/appdata/clientdbts"
                            )
                            changed = True
                        if task.get("working_dir") and "/home/ddp/clientdbts" in task.get(
                            "working_dir"
                        ):
                            task["working_dir"] = task["working_dir"].replace(
                                "/home/ddp/clientdbts", "/mnt/appdata/clientdbts"
                            )
                            changed = True
                        if task.get("profiles_dir") and "/home/ddp/clientdbts" in task.get(
                            "profiles_dir"
                        ):
                            task["profiles_dir"] = task["profiles_dir"].replace(
                                "/home/ddp/clientdbts", "/mnt/appdata/clientdbts"
                            )
                            changed = True

                    if changed:
                        del deployment["id"]
                        del deployment["created"]
                        del deployment["updated"]
                        del deployment["name"]
                        del deployment["flow_id"]
                        del deployment["global_concurrency_limit"]
                        del deployment["labels"]
                        del deployment["last_polled"]
                        del deployment["parameter_openapi_schema"]
                        del deployment["pull_steps"]
                        del deployment["created_by"]
                        del deployment["updated_by"]
                        del deployment["status"]
                        del deployment["schedules"]
                        print(deployment)
                        print("=" * 40)
                        response = requests.patch(
                            f"{BASE_URL}/deployments/{deployment_id}",
                            json=deployment,
                            timeout=20,
                        )
                        try:
                            response.raise_for_status()
                        except Exception:
                            print(response.json())
                            break
                    else:
                        print(f"no change to {deployment_id}")
