import luigi
import pandas as pd
import os
from pipeline.extract import Extract
from pipeline.load import Load
from pipeline.transform import Transform
from pipeline.utils.delete_temp_data import delete_temp
from pipeline.utils.concat_dataframe import concat_dataframes
from pipeline.utils.copy_log import copy_log

DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_LOG = os.getenv("DIR_LOG")


if __name__ == "__main__":

    luigi.build([Extract(),
                 Load(),
                 Transform()],
                 local_scheduler=True)
    

    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    )

    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/load-summary.csv')
    )

    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/transform-summary.csv')
    )

    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'

    )

    delete_temp(
        directory = f'{DIR_TEMP_DATA}'
    )

    delete_temp(
        directory = f'{DIR_TEMP_LOG}'
    )


