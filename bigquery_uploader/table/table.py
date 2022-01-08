import os

from google.cloud import bigquery
from .insert_timeout_error import InsertTimeoutError


class Table:
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
        self.dataset = dataset
        self.table = table

    def insert(self, data: list, timeout: int = 60):
        try:
            self.__bq.insert_rows_json(
                f"{self.dataset}.{self.table}",
                data,
                timeout=timeout
            )
        except TimeoutError as ex:
            raise InsertTimeoutError from ex
