import logging
import os
import json

from .event_payload_extractor import EventPayloadExtractor
from .event_payload_extractor import DataIsNotPresentError
from .data_transformator import DataTransformator
from .table import Table
from .table import InsertTimeoutError


class BigQueryUploader:
    def __init__(self):
        self.__table = Table()

    def process(self, event):
        try:
            data = EventPayloadExtractor.extract(event)
            logging.info(f"Data extracted from event: {data}")

            transformed_data = DataTransformator.transform(json.loads(data))
            logging.info(f"Data was successfully transformed: {transformed_data}")

            timeout = int(os.environ.get("INSERT_TIMEOUT", "60"))
            self.__table.insert(transformed_data, timeout)
            logging.info(f"Transformed data was successfully uploaded to the BigQuery")
        except DataIsNotPresentError as ex:
            logging.exception(ex.message)
        except InsertTimeoutError as ex:
            logging.exception(ex.message)
