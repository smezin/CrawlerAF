from typing import List
from config import QUEUES
from aws_helpers.message_wrapper import delete_messages, receive_messages, send_message
from aws_helpers.queue_wrapper import get_queue

def send_partition_to_queue(partition):
    range_queue = get_queue(QUEUES['partitions'])
    send_message(range_queue, partition)

def get_partition_from_queue() -> List[int]:
    range_queue = get_queue(QUEUES['partitions'])
    message = receive_messages(range_queue, 1, 0)
    if message:
        tasks_range = [int(num) for num in message[0].body.split()]
        delete_messages(range_queue, message)
        return tasks_range
