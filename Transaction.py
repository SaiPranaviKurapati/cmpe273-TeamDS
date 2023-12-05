from datetime import datetime
class Transaction:
    def __init__(self, transaction_id, timestamp, unique_client_id):
        self.transaction_id = transaction_id
        self.unique_client_id = unique_client_id
        self.timestamp = timestamp
        self.read_set = set()
        self.write_set = {}