from collections import deque
import uuid
from access_db import get_rows_num
from config import PARTITION_SETTINGS
from queue_handlers.range_queue_handler import send_partition_to_queue


class PartitionsPopultor:
    def __init__(self) -> None:
        self.partitions = self.set_ranges()

    def set_ranges(self):
        partitions = deque()
        data_len = 105#get_rows_num()
        partitions_n = PARTITION_SETTINGS['count']
        size = data_len // partitions_n
        residue = data_len % partitions_n
        for i in range(partitions_n):
            msg = ' '.join(str(num) for num in [i*size + 1, (i+1)*size])
            partitions.append(msg)
        if residue != 0:
            msg = ' '.join(str(num) for num in [partitions_n*size + 1, partitions_n*size + residue])
            partitions.append(msg)
        return partitions
    
    def push_range_to_queue(self):
        id = uuid.uuid4()
        while self.partitions:
            partition = self.partitions.popleft()
            send_partition_to_queue(partition)
            print(f'PartitionsPopultor sent partition {partition} to partition queue by {id}')
        


