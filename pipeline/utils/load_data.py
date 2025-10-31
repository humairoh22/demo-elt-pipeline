import pandas as pd
from pipeline.utils.connection import db_conn
from sqlalchemy import inspect

def load_to_wh(df, tablename, key_col: list = None, schema: str = None, track_deleted: bool = False):

    try:

        # _, dwh_engine = db_conn()
       _, dwh_engine = db_conn()
       
       table_init = ""
       
       if schema:
        table_init += schema + "."

        table_init += tablename

        df['is_deleted'] = False

        df_columns = list(df.columns)

        
        if not key_col:

            insp = inspect(dwh_engine)
            get_constraint_col = insp.get_unique_constraints(tablename, schema=schema)

            key_col = []
            for col in get_constraint_col:
                key_col.extend(col['column_names'])

        match_col = ",".join([f"{col}" for col in key_col])
        list_col_to_insert = ", ".join([f"{col}" for col in df_columns])
        list_col_to_update = [col for col in df_columns if col not in key_col]
        col_to_update = ", ".join([f'"{col_name}" = EXCLUDED."{col_name}"' for col_name in list_col_to_update])
        

        stmt = f"""
                INSERT INTO {table_init} ({list_col_to_insert})
                SELECT {list_col_to_insert} FROM temp_table
                ON CONFLICT ({match_col}) DO
                UPDATE SET
                {col_to_update},
                update_at = CURRENT_TIMESTAMP
                """
        

        with dwh_engine.begin() as conn:

            conn.exec_driver_sql("DROP TABLE IF EXISTS temp_table")
            conn.exec_driver_sql(
                f"CREATE TEMPORARY TABLE temp_table AS SELECT * FROM {table_init} WHERE false"
            )

            df.to_sql("temp_table", conn, if_exists='append', index=False)
            conn.exec_driver_sql(stmt)
            print(f"Load data to {table_init} success.\n")

            if track_deleted:
               delete_stmt = mark_data_deleted(table_init, key_col)
               conn.exec_driver_sql(delete_stmt)
               
    except Exception as e:
        print(f"Error when loading data to table {tablename}: {e}")



def mark_data_deleted(table_name, key_col):
   
   no_exists_condition = " AND ".join([f"wh.{col} = tmp.{col}" for col in key_col])

   stmt_for_deleted_records = f"""
                                UPDATE {table_name} wh
                                SET is_deleted = TRUE
                                WHERE NOT EXISTS (
                                SELECT 1 FROM temp_table tmp
                                WHERE {no_exists_condition})
                                """
   
   return stmt_for_deleted_records