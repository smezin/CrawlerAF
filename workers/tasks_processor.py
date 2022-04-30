import time
import uuid
from queue_handlers.tasks_queue_handler import get_task_from_queue


class TaskProcessor:
    def __init__(self) -> None:
        self.task : str = None

    def process_current_task(self, process_id: str = 'anonymous') -> None:
        print(f'{process_id} is processing task: {self.task}')
        self.task = None

    def process_tasks(self) -> None:
        id = uuid.uuid4()
        self.task = get_task_from_queue()
        while True:
            self.task = get_task_from_queue()
            if self.task is None:
                print(f'processor {id} is Idle')
                time.sleep(5)
                continue
            self.process_current_task(id)
