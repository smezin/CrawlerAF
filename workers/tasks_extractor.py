import uuid
from collections import deque
from typing import Deque
from access_db import get_rows_range
from queue_handlers.range_queue_handler import get_range_from_queue
from queue_handlers.tasks_queue_handler import send_tasks_to_queue


class TasksExtractor:
    def __init__(self) -> None:
        self.tasks : Deque[str] = deque() 

    def push_tasks_to_queue(self) -> None:
        id = uuid.uuid4()
        for _ in range(2): #while tasks_range
            tasks_range = get_range_from_queue()
            if tasks_range:
                start = tasks_range[0]
                end = tasks_range[1]
                self.tasks = get_rows_range(start, end)
                send_tasks_to_queue(self.tasks)
                print(f'{id} sent task {start} to {end} to queue')

