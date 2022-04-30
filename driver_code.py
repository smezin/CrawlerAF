import multiprocessing
from queue_handlers.queues_setup import delete_queues, setup_queues
from queue_handlers.range_queue_handler import populate_range_queue
from workers.tasks_extractor import TasksExtractor
from workers.tasks_processor import TaskProcessor

# Set instance of populator worker
populator_worker = multiprocessing.Process(target=populate_range_queue)
# Set 2 instaces of extraction worker
extractor = TasksExtractor()
extraction_worker1 = multiprocessing.Process(target=extractor.push_tasks_to_queue)
extraction_worker2 = multiprocessing.Process(target=extractor.push_tasks_to_queue)

# Set 2 instances of processing worker
processor = TaskProcessor()
processing_worker1 = multiprocessing.Process(target=processor.process_tasks)
processing_worker2 = multiprocessing.Process(target=processor.process_tasks)

# Multiprocess 
if __name__ == '__main__':  
    try:
        # Setup queues
        setup_queues()

        populator_worker.start()
        extraction_worker1.start()
        extraction_worker2.start()
        processing_worker1.start()
        processing_worker2.start()

        populator_worker.join()
        extraction_worker1.join()
        extraction_worker2.join()
        processing_worker1.join()
        processing_worker2.join()
    except KeyboardInterrupt:
        delete_queues()
        
    delete_queues()


