import os
import sqlite3

db_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'Northwind_small.sqlite')
con = sqlite3.connect(db_path)

def extract_from_table(table_name:str):
    cur = con.cursor()
    res = cur.execute(f'SELECT * FROM "{table_name}"')
    rows = res.fetchall()
    column_names = [desc[0] for desc in res.description]
    return [column_names] + list(rows)