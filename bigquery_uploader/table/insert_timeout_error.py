class InsertTimeoutError(TimeoutError):
    def __init__(self):
        self.message = "Inserting rows to BigQuery table timed out"
