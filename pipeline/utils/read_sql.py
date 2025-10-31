import pandas as pd
from pipeline.utils.connection import db_conn

def read_sql_file(tablename: str):
    

    src_engine,_ = db_conn()

    query = f"SELECT * FROM {tablename}"

    read_query = pd.read_sql(query, con=src_engine)
    
    print(f"Extract table {tablename} Success")

    return read_query