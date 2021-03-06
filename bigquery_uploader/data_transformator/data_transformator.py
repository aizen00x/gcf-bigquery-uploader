class DataTransformator:
    @staticmethod
    def transform(data: list) -> list:
        """
        Clean and process data
        :param list data: list of entries with data about country's covid situation
        :return: list with transformed data
        """
        def is_valid(item: dict) -> bool:
            """
            Check if item has lastUpdate field set and that recovered amount is smaller than
            confirmed.
            :param dict item: entry with information about country's situation
            :returns: boolean value that represents validity of provided entry
            """
            return item["lastUpdate"] and item["recovered"] <= item["confirmed"]

        result = []
        max_severity = 0

        for item in data:
            if is_valid(item):
                del item["code"]
                del item["latitude"]
                del item["longitude"]
                del item["lastChange"]

                item["timestamp"] = item["lastUpdate"]
                del item["lastUpdate"]

                item["ill"] = item["confirmed"] - item["deaths"] - item["recovered"]

                if item["confirmed"] is not 0:
                    item["severity"] = 1 - item["recovered"] / item["confirmed"]
                else:
                    item["severity"] = 0

                if item["severity"] > max_severity:
                    max_severity = item["severity"]

                result.append(item)

        for item in result:
            item["severity"] /= max_severity
            item["severity"] *= 100

        return result
