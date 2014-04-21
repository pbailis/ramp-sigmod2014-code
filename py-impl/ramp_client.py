
from bloom_filter import BloomFilter
from collections import defaultdict
from data_item import DataItem
from ramp_server import Partition, RAMPAlgorithm

BLOOM_FILTER_SIZE = 20
BLOOM_FILTER_HASHES = 4

class Client:
    def __init__(self, id, partitions, algorithm):
        assert(id < 1024)
        self.id = id
        self.sequence_number = 0
        self.partitions = partitions
        self.algorithm = algorithm

    def key_to_partition(self, key):
        return self.partitions[hash(key) % len(self.partitions)]

    def next_timestamp(self):
        self.sequence_number += 1
        return self.sequence_number << 10 + self.id        

    def put_all(self, kvps):
        timestamp = self.next_timestamp()

        txn_keys = None
        if self.algorithm == RAMPAlgorithm.Fast:
            txn_keys = kvps.keys()

        bloom_filter = None
        if self.algorithm == RAMPAlgorithm.Hybrid:
            bloom_filter = BloomFilter(BLOOM_FILTER_SIZE, BLOOM_FILTER_HASHES)
            bloom_filter.list_to_bloom(kvps.keys())
            
        for key in kvps:
            self.key_to_partition(key).prepare(key,
                                               DataItem(kvps[key],
                                                        timestamp,
                                                        txn_keys,
                                                        bloom_filter),
                                               timestamp)

        for key in kvps:
            self.key_to_partition(key).commit(key, timestamp)

    def get_all(self, keys):
        results = self.get_all_items(keys)

        # remove metadata
        for key in results:
            if results[key]:
                results[key] = results[key].value

        return results
            
    def get_all_items(self, keys):
        if self.algorithm == RAMPAlgorithm.Fast:
            results = {}
            for key in keys:
                results[key] = self.key_to_partition(key).getRAMPFast(key, None)

            vlatest = defaultdict(lambda: -1)
            for value in results.values():
                if value == None:
                    continue
                for tx_key in value.txn_keys:
                    if vlatest[tx_key] < value.timestamp:
                        vlatest[tx_key] = value.timestamp

            for key in keys:
                if key in vlatest and (results[key] == None or
                                       results[key].timestamp < vlatest[key]):
                    results[key] = self.key_to_partition(key).getRAMPFast(key, vlatest[key])

            return results

        elif self.algorithm == RAMPAlgorithm.Small:
            ts_set = set()

            for key in keys:
                last_commit = self.key_to_partition(key).getRAMPSmall(key, None)
                if last_commit:
                    ts_set.add(last_commit)

            results = {}
            for key in keys:
                results[key] = self.key_to_partition(key).getRAMPSmall(key, ts_set)

            return results

        elif self.algorithm == RAMPAlgorithm.Hybrid:
            results = {}
            for key in keys:
                results[key] = self.key_to_partition(key).getRAMPHybrid(key, None)

            for key in keys:
                current_result = results[key]

                key_ts_set = set()
                for value in results.values():
                    if value and (not current_result or value.timestamp > current_result.timestamp):
                        key_ts_set.add(value.timestamp)

                if len(key_ts_set) > 0:
                    second_round_result = self.key_to_partition(key).getRAMPHybrid(key,
                                                                                   key_ts_set)
                    if second_round_result:
                        results[key] = second_round_result

            return results
            

            
            
