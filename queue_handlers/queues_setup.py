import logging
from aws_helpers.queue_wrapper import create_queue, get_queue, remove_queue
from config import QUEUES

logger = logging.getLogger(__name__)

def setup_queues():
    """
    Set up 'partitions' and 'tasks' queues 
    """
    try:
        queue_name = QUEUES['partitions']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.warn(f'Failed creating queue: {queue_name}')

    try:
        queue_name = QUEUES['tasks']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.warn(f'Failed creating queue: {queue_name}')

def delete_queues():
    """
    Delete 'partitions' and 'tasks' queues
    """
    ranges_queue = get_queue(QUEUES['partitions'])
    remove_queue(ranges_queue)
    tasks_queue = get_queue(QUEUES['tasks'])
    remove_queue(tasks_queue)
