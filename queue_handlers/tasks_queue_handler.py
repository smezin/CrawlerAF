from typing import List
from aws_helpers.message_wrapper import delete_messages, receive_messages, send_message
from aws_helpers.queue_wrapper import get_queue
from config import QUEUES

def send_tasks_to_queue(tasks: List[any]) -> None:
    tasks_queue = get_queue(QUEUES['tasks'])
    while tasks:
        send_message(tasks_queue, str(tasks.popleft()))

def get_task_from_queue() -> str:
    tasks_queue = get_queue(QUEUES['tasks'])
    messages = receive_messages(tasks_queue, 1, 0)
    if messages:
        task = str(messages[0].body)
        delete_messages(tasks_queue, messages)
        return task
