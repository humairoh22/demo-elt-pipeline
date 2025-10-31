import os
from dotenv import load_dotenv
from sqlalchemy import inspect, create_engine



def db_conn():

    try:
        src_db = os.getenv("SRC_DB")
        src_host = os.getenv("SRC_HOST")
        src_user= os.getenv("SRC_USER")
        src_password = os.getenv("SRC_PASSWORD")


        dwh_db = os.getenv("DWH_POSTGRES_DB")
        dwh_host = os.getenv("DWH_POSTGRES_HOST")
        dwh_user = os.getenv("DWH_POSTGRES_USER")
        dwh_password = os.getenv("DWH_POSTGRES_PASSWORD")
        dwh_port = os.getenv("DWH_POSTGRES_PORT")



        src_conn = create_engine(f"mysql+pymysql://{src_user}:{src_password}@{src_host}/{src_db}")

        # src_conn = mysql.connect(host=src_host,
        #                  user=src_user,
        #                  password= src_password,
        #                  db= src_db)

        dwh_conn = create_engine(f"postgresql://{dwh_user}:{dwh_password}@{dwh_host}:{dwh_port}/{dwh_db}")


        return src_conn, dwh_conn
    
    except Exception as e:
        print(f"Error when connecting to database: {e}")