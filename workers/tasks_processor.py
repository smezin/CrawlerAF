import time
import uuid
from config import QUEUES
from queue_handlers.tasks_queue_handler import get_task_from_queue

class TaskProcessor:
    def __init__(self) -> None:
        self.task : str = None

    def process_tasks(self) -> None:
        """
        Process tasks from task queue. if tasks queue is empty there will be retries as configured
        in intervals of configured number of seconds
        """
        id = uuid.uuid4()
        def process_current_task() -> None:
            # Call processing
            print(f'TaskProcessor processing task: {self.task} by {id}')
            self.task = None

        retries = QUEUES['retries']
        while retries > 0:
            self.task = get_task_from_queue()
            if self.task is None:
                retries -= 1
                print(f'TaskProcessor {id} is Idle, will retry {retries} more times')
                time.sleep(QUEUES['sleep'])
                continue
            process_current_task()
