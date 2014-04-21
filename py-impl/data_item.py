
class DataItem:
    def __init__(self, value, timestamp, txn_keys=None, bloom_filter=None):
        self.value = value
        self.timestamp = timestamp
        self.txn_keys = txn_keys
        self.bloom_filter = bloom_filter  
