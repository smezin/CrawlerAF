import logging
from aws_helpers.queue_wrapper import create_queue
from config import QUEUES

logger = logging.getLogger(__name__)

def setup_queues():
    try:
        queue_name = QUEUES['ranges']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.warn(f'Failed creating queue: {queue_name}')

    try:
        queue_name = QUEUES['tasks']
        create_queue(queue_name, QUEUES['attributes'])
    except:
        logger.warn(f'Failed creating queue: {queue_name}')