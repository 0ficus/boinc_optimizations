import requests
import time
import uuid

SERVER_URL = "http://localhost:8000"
CLIENT_ID = str(uuid.uuid4())


def execute_task(data: str, func_code: str) -> str:
    try:
        local_vars = {"data": data}
        exec(func_code, globals(), local_vars)
        return local_vars.get("result", "No result returned")
    except Exception as e:
        return f"Error: {e}"


def run_client():
    while True:
        try:
            response = requests.get(f"{SERVER_URL}/get_job?client_id={CLIENT_ID}").json()
            if response.get("status") == "no_jobs_available":
                time.sleep(5)
                continue

            job_id, data, func_code = response["job_id"], response["data"], response["func_code"]
            result = execute_task(data, func_code)
            requests.post(
                f"{SERVER_URL}/submit_result",
                json={"job_id": job_id, "client_id": CLIENT_ID, "result": str(result)}
            )
        except Exception as e:
            time.sleep(5)


if __name__ == "__main__":
    run_client()
