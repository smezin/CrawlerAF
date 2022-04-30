# CrawlerAF - MultiProcess tasks handler
Purpose: Process 1M rows sql db
## Logic: 
    Built for multiprocessing on both sides of the tasks queue. Allows horizontally scaling from filling the 
    tasks queue from the db, and same horizontally scaling for processing tasks from queue
    1. Set partitions list of original db, each sized as db size / configured num of partitions
    2. Send partitions list (e.g '1 100') to partitions queue
    3. Consume partitions (ranges) and extract corresponding rows from db
    4. MULTIPROCESS workers: Send tasks to tasks queue, where each row represents a task. 
    5. MULTIPROCESS workers: Extract tasks from tasks queue and process it
    In case a worker encounters an empty queue, it will retry for few times (as configured in config.py)
## For running:
    1. IDE of environment must be set with AWS credentials that allow creating/deleting queues (SQS)
    2. A connection string to a db with some rows, each with 'id' field as int in consecutive order
### aws_helpers:
    AWS scripts for wrapping queues and messages
### queue_handlers:
    Queue access layer
### access_db:
    Db access layer
### workers:
    Well, someone has to work around here
    
