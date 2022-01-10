import os

from google.cloud import bigquery
from .insert_timeout_error import InsertTimeoutError


class Table:
    """
    Establishes connection with BigQuery and is responsible for interaction with a specific table
    """
    def __init__(self,
                 dataset: str = os.environ.get("DATASET", ""),
                 table: str = os.environ.get("TABLE", "")):
        if not dataset:
            raise Exception("Dataset was not provided. Pass it to the constructor or set "
                            "'DATASET' environment variable")

        if not table:
            raise Exception("Table was not provided. Pass it to the constructor or set "
                            "'Table' environment variable")

        self.__bq = bigquery.Client()
        self.__dataset = dataset
        self.__table = table

    def insert(self, data: list, timeout: int = 60) -> None:
        """
        Insert list of entries to table
        :param list data: List of entries to insert in the table
        :param int timeout: Timeout for insertion. Default is 60 seconds
        :return: None
        :raises: InsertTimeoutError
        """
        try:
            self.__bq.insert_rows_json(
                f"{self.__dataset}.{self.__table}",
                data,
                timeout=timeout
            )
        except TimeoutError as ex:
            raise InsertTimeoutError from ex
