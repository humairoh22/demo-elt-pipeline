import luigi
import os
import pandas as pd
import time
import logging
import subprocess as sp
from pipeline.load import Load
from datetime import datetime

DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_DBT_TRANSFORM = os.getenv("DIR_DBT_TRANSFORM")

class Transform(luigi.Task):

    def requires(self):
        return Load()
    
    def run(self):
        
        start_time = time.time()

        logging.info("===============================================STARTING TRANSFORM DATA===============================================")

        try:
            with open(
                f'{DIR_TEMP_LOG}/logs.log', mode='a') as f:
                sp.run(f"cd {DIR_DBT_TRANSFORM} && dbt run",
                stdout = f,
                stderr  =sp.PIPE,
                text = True,
                shell = True,
                check = True
                )

            end_time = time.time()
            execution_time = end_time - start_time

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status': ['SUCCESS'],
                'execution_time': [execution_time]
            }
            
            summary = pd.DataFrame(summary_data)
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index=False)

        except Exception:
            logging.error(f"Transform to all dimension and fact table - FAILED")

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status': ['FAILED'],
                'execution_time': [0]
            }

            summary = pd.DataFrame(summary_data)
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index=False)

            logging.error("Transform tables - FAILED")
            raise Exception("Failed Transforming tables")
        
        logging.info("===============================================ENDING TRANSFORM DATA===============================================")

    def output(self):
        return([luigi.LocalTarget(f'{DIR_TEMP_DATA}/transform-summary.csv'),
                luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log')])