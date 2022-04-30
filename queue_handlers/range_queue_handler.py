from typing import List
from access_db import get_rows_num
from config import PARTITION_SETTINGS, QUEUES
from aws_helpers.message_wrapper import delete_messages, receive_messages, send_message
from aws_helpers.queue_wrapper import get_queue

def populate_range_queue() -> None:
    range_queue = get_queue(QUEUES['ranges'])
    data_len = get_rows_num()
    partitions_n = PARTITION_SETTINGS['count']
    size = data_len // partitions_n
    residue = data_len % partitions_n
    for i in range(50): #(partitions_n):
        msg = ' '.join(str(num) for num in [i*size + 1, (i+1)*size])
        send_message(range_queue, msg)
        print(f'sent packet #{i}')
    if residue != 0:
        msg = ' '.join(str(num) for num in [partitions_n*size + 1, partitions_n*size + residue])
        send_message(range_queue, msg)

def get_range_from_queue() -> List[int]:
    range_queue = get_queue(QUEUES['ranges'])
    message = receive_messages(range_queue, 1, 0)
    if message:
        tasks_range = [int(num) for num in message[0].body.split()]
        delete_messages(range_queue, message)
        return tasks_range
