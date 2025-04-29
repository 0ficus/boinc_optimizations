import requests
import time
import uuid

SERVER_URL = "http://localhost:8000"
CLIENT_ID = str(uuid.uuid4())

TIMESTAMP = 2
ITERATIONS = 10


def compute(data: str) -> str:
    time.sleep(TIMESTAMP)
    return f"result_for_{data}"


def run_client():
    for iteration in range(ITERATIONS):
        response = requests.get(f"{SERVER_URL}/get_job?client_id={CLIENT_ID}").json()
        if response.get("status") == "no_jobs_available":
            time.sleep(5)
            continue

        job_id, data = response["job_id"], response["data"]
        result = compute(data)
        submit_data = {
            "job_id": job_id,
            "client_id": CLIENT_ID,
            "result": result
        }
        response = requests.post(f"{SERVER_URL}/submit_result", json=submit_data)


if __name__ == "__main__":
    run_client()
