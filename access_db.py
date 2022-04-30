import pyodbc
from collections import deque
from typing import Deque
from config import CONNECTIONS

def get_rows_num() -> int:
    """
    Get number of rows in db.
    return: number of rows
    """
    connection = pyodbc.connect(CONNECTIONS['default'])
    cursor = connection.cursor()
    query = 'SELECT COUNT (*) FROM DataTable'
    cursor.execute(query)
    rows =  (list(cursor)[0][0])
    cursor.close()
    connection.close()
    return rows
    

def get_rows_range(start: int, end: int) -> Deque[str]:
    """
    Extracts from db rows with id in range start->end inclusive
    return: double ended queue of rows from db
    """
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
  