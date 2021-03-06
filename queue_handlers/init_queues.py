from distutils.log import error
import logging
from aws_helpers.queue_wrapper import create_queue, get_queue, remove_queue
from config import QUEUES

logger = logging.getLogger(__name__)

def init_queues():
    """
    Set up 'partitions' and 'tasks' queues 
    """
    try:
        queue_name = QUEUES['partitions']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.error(f'Failed creating queue: {queue_name}')
        raise error
    else:
        print(f'{queue_name} was created successfully')
    try:
        queue_name = QUEUES['tasks']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.error(f'Failed creating queue: {queue_name}')
        raise error
    else:
        print(f'{queue_name} was created successfully')


def delete_queues():
    """
    Delete 'partitions' and 'tasks' queues
    """
    try:
        ranges_queue = get_queue(QUEUES['partitions'])
        remove_queue(ranges_queue)
        tasks_queue = get_queue(QUEUES['tasks'])
        remove_queue(tasks_queue)
    except:
        logger.error(f'Failed deleting queues')
        raise error
    else:
        print('Queues deleted')
