import multiprocessing
from queue_handlers.init_queues import delete_queues, init_queues
from workers.partitions_populator import PartitionsPopultor
from workers.tasks_extractor import TasksExtractor
from workers.tasks_processor import TaskProcessor

workers = []
# Set instace of populator worker
populator = PartitionsPopultor()
workers.append(multiprocessing.Process(target=populator.push_range_to_queue))

# Set 2 instaces of extraction worker
extractor = TasksExtractor()
for _ in range(2):
    workers.append(multiprocessing.Process(target=extractor.push_tasks_to_queue))

# Set 2 instances of processing worker
processor = TaskProcessor()
for _ in range(2):
    workers.append(multiprocessing.Process(target=processor.process_tasks))

# Multiprocess 
if __name__ == '__main__':  
    try:
        init_queues()

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        delete_queues()

    delete_queues()