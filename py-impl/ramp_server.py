
from collections import defaultdict
from threading import Lock

class RAMPAlgorithm:
    Fast, Small, Hybrid = range(3)

class Partition:
    def __init__(self):
        # map of maps: key -> [version -> value]
        self.versions = defaultdict(lambda: defaultdict(lambda: None))
        self.lastCommit = defaultdict(lambda: None)

        # used for synchronizing last-writer-wins
        self.lwwLock = Lock()

    def prepare(self, key, value, timestamp):
        self.versions[key][timestamp] = value

    def commit(self, key, timestamp):
        self.lwwLock.acquire()
        if self.lastCommit[key] < timestamp:
            self.lastCommit[key] = timestamp
        self.lwwLock.release()

    def getRAMPFast(self, key, ts_required):
        if ts_required == None:
            return self.versions[key][self.lastCommit[key]]
        else:
            return self.versions[key][ts_required]

    def getRAMPSmall(self, key, ts_set):
        if ts_set == None:
            return self.lastCommit[key]
        else:
            presentTimestamps = self.versions[key].keys()
            presentTimestamps.sort()
            presentTimestamps.reverse()
            for version in presentTimestamps:
                if version in ts_set:
                   return self.versions[key][version]

            return None

    def getRAMPHybrid(self, key, ts_set):
        if ts_set == None:
           return self.versions[key][self.lastCommit[key]]
        else:
           return self.getRAMPSmall(key, ts_set)

    

    
