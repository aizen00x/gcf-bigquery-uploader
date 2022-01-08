class DataIsNotPresentError(KeyError):
    """
    Exception for cases when event does not contain any data
    """
    def __init__(self):
        self.message = "Data is not present in the payload"
