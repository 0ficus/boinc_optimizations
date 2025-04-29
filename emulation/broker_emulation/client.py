import pika
import json
import uuid

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

TASK_QUEUE = 'task_queue'
RESULT_QUEUE = 'result_queue'

channel.queue_declare(queue=TASK_QUEUE, durable=True)
channel.queue_declare(queue=RESULT_QUEUE)


def execute_task(data: str, func_code: str):
    try:
        local_vars = {"data": data, "result": None}
        exec(func_code, {"__builtins__": None}, local_vars)
        return str(local_vars.get("result", "No result"))
    except Exception as e:
        return f"Error: {e}"


def callback(ch, method, properties, body):
    task = json.loads(body)
    result = execute_task(task['data'], task['func_code'])
    response = {
        'task_id': task['task_id'],
        'result': result,
        'client_id': f"client-{uuid.uuid4().hex[:6]}"
    }
    ch.basic_publish(
        exchange='',
        routing_key=RESULT_QUEUE,
        body=json.dumps(response),
        properties=pika.BasicProperties(delivery_mode=2)
    )


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=TASK_QUEUE, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
