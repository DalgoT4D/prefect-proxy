import requests
import argparse


def clear_offline_workers(work_pool_name: str):
    """
    Clears offline workers from the specified work pool.

    Args:
        work_pool_name (str): The name of the work pool.
        max_workers_to_delete (int): Maximum number of workers to delete.
    """
    payload = {"workers": {"status": {"any_": ["OFFLINE"]}}}

    # Fetch offline workers
    res = requests.post(
        f"http://localhost:4200/api/work_pools/{work_pool_name}/workers/filter", json=payload
    )
    workers_to_delete = res.json()
    print(f"Found offline workers: {len(workers_to_delete)}")

    # Delete offline workers
    for i, worker in enumerate(workers_to_delete):
        name = worker["name"]
        status = worker["status"]
        print(f"iter: {i} Deleting worker {name} with status: {status}")
        if status == "OFFLINE":
            res = requests.delete(
                f"http://localhost:4200/api/work_pools/{work_pool_name}/workers/{name}"
            )
            print(f"iter: {i} Deleted worker {name} with status code: {res.status_code}")


# Call the function
if __name__ == "__main__":
    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Clear offline workers from a work pool.")
    parser.add_argument(
        "work_pool_name",
        type=str,
        help="The name of the work pool to clear offline workers from.",
        default="prod_dalgo_work_pool",
    )
    args = parser.parse_args()

    # Call the function with the provided work pool name
    clear_offline_workers(work_pool_name=args.work_pool_name)
