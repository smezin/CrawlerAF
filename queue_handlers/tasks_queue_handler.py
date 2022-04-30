from typing import List
from aws_helpers.message_wrapper import delete_messages, receive_messages, send_message
from aws_helpers.queue_wrapper import get_queue
from config import QUEUES

def send_tasks_to_queue(tasks: List[any]) -> None:
    """
    Sends tasks (messages) to tasks queue.
    param tasks: list of objects that can be stringified with str
    """
    tasks_queue = get_queue(QUEUES['tasks'])
    while tasks:
        send_message(tasks_queue, str(tasks.popleft()))

def get_task_from_queue() -> str:
    """
    Gets a task from tasks queue.
    return: task as string
    """
    tasks_queue = get_queue(QUEUES['tasks'])
    messages = receive_messages(tasks_queue, 1, 0)
    if messages:
        task = str(messages[0].body)
        delete_messages(tasks_queue, messages)
        return task
