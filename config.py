"""
Environment must be set with valid AWS user credentials with permissions to access AWS SQS
"""

CONNECTIONS = dict(
    default = 'Driver={SQL Server};Server=SHAHAF-LP-HHS;Database=MilDb;Trusted_Connection=yes;'
)

PARTITION_SETTINGS = dict(
    count = 100000
)

QUEUES = dict(
    partitions = 'PartitionQueue.fifo',
    tasks = 'TasksQueue.fifo',
    attributes = {
        'MaximumMessageSize': str(4096),
        'ReceiveMessageWaitTimeSeconds': str(10),
        'VisibilityTimeout': str(300),
        'FifoQueue': str(True),
        'ContentBasedDeduplication': str(True)
    },
    retries = 10,
    sleep = 10
)
