import requests
import time
import uuid
import threading
import ast
from functools import partial

SERVER_URL = "http://localhost:8000"
CLIENT_AMOUNT = 5


def execute_task_safely(data: str, func_code: str) -> str:
    try:
        tree = ast.parse(func_code)
        local_vars = {"data": data, "result": None}
        exec(func_code, {"__builtins__": None}, local_vars)
        return str(local_vars.get("result", "No result returned"))
    except Exception as e:
        return f"Error: {e}"


def client_worker(client_id: str):
    while True:
        try:
            response = requests.get(f"{SERVER_URL}/get_job?client_id={client_id}", timeout=10).json()
            if response.get("status") == "no_jobs_available":
                time.sleep(5)
                continue

            job_id, data, func_code = response["job_id"], response["data"], response["func_code"]
            result = execute_task_safely(data, func_code)
            requests.post(
                f"{SERVER_URL}/submit_result",
                json={"job_id": job_id, "client_id": client_id, "result": result},
                timeout=10
            )
        except requests.RequestException as e:
            time.sleep(5)
        except Exception as e:
            print(f"[Client {client_id}] Error: {e}")
            time.sleep(1)


def run_clients(num_clients: int = CLIENT_AMOUNT):
    threads = []
    for i in range(num_clients):
        client_id = f"client-{uuid.uuid4().hex[:6]}"
        thread = threading.Thread(
            target=partial(client_worker, client_id),
            daemon=True
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    run_clients()
