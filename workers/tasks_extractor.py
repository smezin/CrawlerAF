import uuid
import time
from collections import deque
from typing import Deque
from access_db import get_rows_range
from config import QUEUES

from queue_handlers.range_queue_handler import get_partition_from_queue
from queue_handlers.tasks_queue_handler import send_tasks_to_queue

class TasksExtractor:
    def __init__(self) -> None:
        self.tasks : Deque[str] = deque() 

    def push_tasks_to_queue(self) -> None:
        """
        pushes tasks to task queue. if ranges queue is empty there will be retries as configured
        in intervals of configured number of seconds
        """
        id = uuid.uuid4()
        retries = QUEUES['retries']
        while retries > 0:
            tasks_range = get_partition_from_queue()
            if tasks_range is None:
                retries -= 1
                print(f'TasksExtractor {id} is Idle, will retry {retries} more times')
                time.sleep(QUEUES['sleep'])
                continue
            start = tasks_range[0]
            end = tasks_range[1]
            self.tasks = get_rows_range(start, end)
            send_tasks_to_queue(self.tasks)
            print(f'TasksExtractor sent tasks {start} to {end} to tasks queue by {id}')

