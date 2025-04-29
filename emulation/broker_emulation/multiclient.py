import pika
import json
import uuid
import threading
from functools import partial

RABBITMQ_HOST = 'localhost'
TASK_QUEUE = 'task_queue'
RESULT_QUEUE = 'result_queue'

class RabbitMQClient:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(RABBITMQ_HOST)
        )
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=TASK_QUEUE, durable=True)
        self.channel.queue_declare(queue=RESULT_QUEUE)

    def execute_task(self, data: str, func_code: str) -> str:
        try:
            local_vars = {"data": data, "result": None}
            exec(func_code, {"__builtins__": None}, local_vars)
            return str(local_vars.get("result", "No result"))
        except Exception as e:
            return f"Error: {e}"

    def process_task(self, ch, method, properties, body):
        task = json.loads(body)
        result = self.execute_task(task['data'], task['func_code'])
        response = {
            'task_id': task['task_id'],
            'result': result,
            'client_id': self.client_id
        }
        self.channel.basic_publish(
            exchange='',
            routing_key=RESULT_QUEUE,
            body=json.dumps(response),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start_consuming(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(
            queue=TASK_QUEUE,
            on_message_callback=self.process_task,
            auto_ack=False
        )
        print(f"[{self.client_id}] Waiting for tasks...")
        self.channel.start_consuming()

def run_client(client_id: str):
    client = RabbitMQClient(client_id)
    client.start_consuming()


if __name__ == "__main__":
    NUM_CLIENTS = 100
    threads = []

    for i in range(NUM_CLIENTS):
        client_id = f"client-{uuid.uuid4().hex[:6]}"
        thread = threading.Thread(
            target=partial(run_client, client_id),
            daemon=True
        )
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
