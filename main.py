'''
name: jason
date: Aug, 2024

'''
import logging
# import requests
# import pprint
from util.helper import get_logging_format, write_to_csv
from url_extract import fetch_data
from load_to_bq import bq_loader
from export_to_gsheet import bq_query

logger: logging.Logger = get_logging_format()



# Main Execution
def main_func():
    """
    Main function
    """

    # step 1: fetch data from URL
    final_df = fetch_data()

    # step 2: load data into bigquery 
    bq_loader(final_df)

    # step 3: run query and export data into gsheet
    bq_query()


if __name__ == "__main__":
    main_func()

