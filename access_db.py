import pyodbc
from collections import deque
from typing import Deque
from config import CONNECTIONS

def get_rows_num():
    cnxn = pyodbc.connect(CONNECTIONS['default'])
    cursor = cnxn.cursor()
    query = 'SELECT COUNT (*) FROM DataTable'
    cursor.execute(query)
    return (list(cursor)[0][0])

def get_rows_range(start: int, end: int) -> Deque[str]:
    cnxn = pyodbc.connect(CONNECTIONS['default'])
    cursor = cnxn.cursor()
    query = f'SELECT * FROM DataTable WHERE id BETWEEN {start} AND {end}'
    cursor.execute(query)
    rows = deque()
    for row in cursor:
        rows.append(row)
    return rows
  