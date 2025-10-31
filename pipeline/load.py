import luigi
import logging
import pandas as pd
import time
import os
from datetime import datetime
from pipeline.utils.load_data import load_to_wh
from pipeline.extract import Extract


DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")


class Load(luigi.Task):
   
    def requires(self):
        return Extract()
    
    def run(self):
        logging.basicConfig(
                filename = f'{DIR_TEMP_LOG}/logs.log',
                level = logging.INFO,
                format = '%(asctime)s - %(levelname)s - %(message)s')

        try:
            
            delivery_man = pd.read_csv(self.input()[0].path)
            packinglist = pd.read_csv(self.input()[1].path)
            transferstatus = pd.read_csv(self.input()[2].path)
            deliveryproof = pd.read_csv(self.input()[3].path)
            customers = pd.read_csv(self.input()[4].path)
            expedition = pd.read_csv(self.input()[5].path)
            license_plate = pd.read_csv(self.input()[6].path)
            office_address = pd.read_csv(self.input()[7].path)

            logging.info(f"Read Extracted data - SUCCESS.")
                        
        except Exception:
            logging.error(f"Read Extracted data - FAILED")      
            raise Exception("Failed to read Extracted data")  
        
        start_time = time.time()
        logging.info("===============================================STARTING LOAD DATA===============================================")
        
        try:
            try:
                
                target_table = {'delivery_man':delivery_man,
                                'packinglist':packinglist,
                                'transferstatus':transferstatus,
                                'deliveryproof':deliveryproof,
                                'customers':customers,
                                'expedition':expedition,
                                'license_plate':license_plate,
                                'office_address':office_address}

                for tb_name, data in target_table.items():
                    load_to_wh(data,
                                tb_name,
                                schema='public',
                                track_deleted=True)
                    
                    logging.info(f"Load {tb_name} - SUCCESS")

                logging.info(f'Load all tables to DWH - SUCCESS')

            except Exception:           
                logging.error(f"Load Extracted data - FAILED")

            
            end_time = time.time()
            execution_time = end_time - start_time

            summary_data = {'timestamp': [datetime.now()],
                            'task': ['Load'],
                            'status': ['SUCCESS'],
                            'execution_time': [execution_time]
                            }

            summary = pd.DataFrame(summary_data)
            summary.to_csv(f'{DIR_TEMP_DATA}/load-summary.csv', index=False)
    
        except Exception:

            summary_data = {'timestamp': [datetime.now()],
                            'task': ['Load'],
                            'status': ['FAILED'],
                            'execution_time': [0]
                            }

            summary = pd.DataFrame(summary_data)
            summary.to_csv(f'{DIR_TEMP_DATA}/load-summary.csv', index=False)

            logging.info(f'Load all tables to DWH - FAILED')
        
        logging.info("===============================================ENDING LOAD DATA===============================================")

    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv'),
                luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log')]





