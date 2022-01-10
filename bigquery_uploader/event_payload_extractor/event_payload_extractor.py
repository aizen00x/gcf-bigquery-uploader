import base64
from .data_is_not_present_error import DataIsNotPresentError


class EventPayloadExtractor:
    @staticmethod
    def extract(event) -> str:
        """
        Extract data from event
        :param dict event: Event which is provided by Google Cloud when function is invoked
        :return: Extracted and decoded data
        :raises: DataIsNotPresentError
        """

        if "data" in event:
            return base64.b64decode(event['data']).decode("utf-8")
        else:
            raise DataIsNotPresentError
