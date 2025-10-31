import luigi
import pandas as pd
from datetime import datetime
import logging
import time
import os
from pipeline.utils.read_sql import read_sql_file
from dotenv import load_dotenv

load_dotenv()

DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")

class Extract(luigi.Task):

    tb_source = [
            'delivery_man',
            'packinglist',
            'transferstatus',
            'deliveryproof',             
            'customers',
            'expedition',
            'license_plate',
            'office_address'
             ]
    
    def requires(self):
        pass

    def run(self):
        try:
            # configure logging
            logging.basicConfig(filename=f'{DIR_TEMP_LOG}/logs.log',
                                level=logging.INFO,
                                format='%(asctime)s - %(levelname)s - %(message)s')

            start_time = time.time()
            logging.info("===============================================STARTING EXTRACT DATA===============================================")

            for index, tb_name in enumerate(self.tb_source):
                try:
                    RENAME_COL = {'no_invoice':'sj_numbers', 
                        'concat_inv':'no_sj', 
                        'pod':'pod_image', 
                        'timestamp':'pod_date',
                        'created_at':'src_created_at',
                        'update_at':'src_update_at'}
                    
                    # read data from source
                    df = read_sql_file(tb_name)
                    df = df.rename(columns=RENAME_COL)

                    # save data to csv
                    df.to_csv(f"{DIR_TEMP_DATA}/{tb_name}.csv", index=False)

                    logging.info(f"EXTRACT {tb_name} - SUCCESS.")

                except Exception:
                    logging.error(f"EXTRACT {tb_name} - FAILED.")
                    raise Exception(f"Failed to extract '{tb_name} tables.")

            logging.info(f"Extract all tables from sources - SUCCESS.")

            end_time = time.time()
            execution_time = end_time - start_time

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Extract'],
                'status': ['SUCCESS'],
                'execution_time': [execution_time]
            }

            summary = pd.DataFrame(summary_data)
            summary.to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index=False)
        
        except Exception:
            logging.info(f"Extract All tables from sources - FAILED")

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Extract'],
                'status': ['FAILED'],
                'execution_time': [0]
            }

            summary = pd.DataFrame(summary_data)
            summary.to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index=False)

            raise Exception(f"Failed to execute the task!")

        logging.info("===============================================ENDING EXTRACT DATA===============================================")

    def output(self):

        output = []
        
        for tb_name in self.tb_source:
            output.append(luigi.LocalTarget(f"{DIR_TEMP_DATA}/{tb_name}.csv"))

        output.append(luigi.LocalTarget(f"{DIR_TEMP_DATA}/extract-summary.csv"))

        output.append(luigi.LocalTarget(f"{DIR_TEMP_LOG}/logs.log"))

        return output