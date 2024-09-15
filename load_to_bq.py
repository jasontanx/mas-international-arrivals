'''
load to bigquery

'''

import logging
# import time
from datetime import datetime
# import pandas as pd
# from pandas import DataFrame
# import csv

# import requests
# import json
# import os
# from typing import List
from google.cloud import bigquery
# from google.cloud.bigquery.schema import SchemaField
from util.helper import get_logging_format, write_to_csv
from config.conf import project_id, dataset_id, table_id, key_path, filename

logger: logging.Logger = get_logging_format()


def bq_loader(data):
    '''
    load to bigquery

    '''
    # output data in csv format

    logger.info("writing to csv...")

    write_to_csv(data, filename)

    logger.info("Data generated in csv")

    data['ingested_at'] = datetime.now()

    logger.info("ingesting to bq...")
    bq_client = bigquery.Client.from_service_account_json(key_path) # config path
    table_bq = f'{project_id}.{dataset_id}.{table_id}'
    _write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    _schema = [
    {
      "description": "Date",
      "mode": "NULLABLE",
      "name": "date",
      "type": "DATE"
    },
    {
      "description": "Country",
      "mode": "NULLABLE",
      "name": "country",
      "type": "STRING"
    },
    {
      "description": "State of Entry",
      "mode": "NULLABLE",
      "name": "soe",
      "type": "STRING"
    },
    {
      "description": "Arrivals",
      "mode": "NULLABLE",
      "name": "arrivals",
      "type": "INTEGER"
    },
    {
      "description": "Arrivals - Male",
      "mode": "NULLABLE",
      "name": "arrivals_male",
      "type": "INTEGER"
    },
    {
      "description": "Arrivals - Female",
      "mode": "NULLABLE",
      "name": "arrivals_female",
      "type": "INTEGER"
    },
    {
      "description": "ingested_at",
      "mode": "NULLABLE",
      "name": "ingested_at",
      "type": "TIMESTAMP"
    }
    ]

    job_config = bigquery.LoadJobConfig(
                schema=_schema,
                write_disposition=_write_disposition,
                autodetect=False,
                # skip_leading_rows=1,
            )

    job = bq_client.load_table_from_dataframe(data, table_bq, job_config=job_config)
    # Wait for the job to complete
    job.result()
    logger.info("Data ingested to BQ -> %s", table_id)

