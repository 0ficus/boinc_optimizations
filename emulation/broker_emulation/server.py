import pika
import uuid
import uvicorn
import json
from fastapi import FastAPI
from pydantic import BaseModel
import threading

app = FastAPI()

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

TASK_QUEUE = 'task_queue'
RESULT_QUEUE = 'result_queue'

channel.queue_declare(queue=TASK_QUEUE, durable=True)
channel.queue_declare(queue=RESULT_QUEUE)


class Task(BaseModel):
    data: str
    func_code: str


def send_task_to_queue(task: Task):
    task_id = str(uuid.uuid4())
    message = {
        'task_id': task_id,
        'data': task.data,
        'func_code': task.func_code
    }
    channel.basic_publish(
        exchange='',
        routing_key=TASK_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2)
    )
    return {"task_id": task_id}


def listen_for_results():
    def callback(ch, method, properties, body):
        result = json.loads(body)

    channel.basic_consume(queue=RESULT_QUEUE, on_message_callback=callback, auto_ack=True)
    channel.start_consuming()


result_thread = threading.Thread(target=listen_for_results, daemon=True)
result_thread.start()


@app.post("/create_task")
def create_task(task: Task):
    return send_task_to_queue(task)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
