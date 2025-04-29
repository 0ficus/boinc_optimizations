import requests
import time
import uuid
import threading

SERVER_URL = "http://localhost:8000"

TIMESTAMP = 2
ITERATIONS = 10
CLIENT_AMOUNT = 100


def compute(data: str) -> str:
    time.sleep(TIMESTAMP)
    return f"result_for_{data}"


def client_worker(client_id: str):
    for iteration in range(ITERATIONS):
        try:
            response = requests.get(f"{SERVER_URL}/get_job?client_id={client_id}").json()
            if response.get("status") == "no_jobs_available":
                time.sleep(5)
                continue

            job_id, data = response["job_id"], response["data"]
            result = compute(data)
            submit_data = {
                "job_id": job_id,
                "client_id": client_id,
                "result": result
            }
            requests.post(f"{SERVER_URL}/submit_result", json=submit_data)

        except Exception as e:
            print(f"[Клиент {client_id}] Ошибка: {e}")
            time.sleep(5)


def run_clients(num_clients: int = 3):
    threads = []
    for _ in range(num_clients):
        client_id = str(uuid.uuid4())
        thread = threading.Thread(target=client_worker, args=(client_id,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == "__main__":
    run_clients(num_clients=CLIENT_AMOUNT)