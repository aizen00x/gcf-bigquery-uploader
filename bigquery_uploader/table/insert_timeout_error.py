class InsertTimeoutError(TimeoutError):
    def __init__(self):
        self.message = "Inserting rows to Big Query table timed out"
