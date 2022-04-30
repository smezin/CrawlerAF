import pyodbc
from collections import deque
from typing import Deque
from config import CONNECTIONS

def get_rows_num() -> int:
    connection = pyodbc.connect(CONNECTIONS['default'])
    cursor = connection.cursor()
    query = 'SELECT COUNT (*) FROM DataTable'
    cursor.execute(query)
    rows =  (list(cursor)[0][0])
    cursor.close()
    connection.close()
    return rows
    

def get_rows_range(start: int, end: int) -> Deque[str]:
    connection = pyodbc.connect(CONNECTIONS['default'])
    cursor = connection.cursor()
    query = f'SELECT * FROM DataTable WHERE id BETWEEN {start} AND {end}'
    cursor.execute(query)
    rows = deque()
    for row in cursor:
        rows.append(row)
    cursor.close()
    connection.close()
    return rows
  