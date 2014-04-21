
# quick and dirty lift from http://glowingpython.blogspot.nl/2013/01/bloom-filter.html

class BloomFilter:
    def __init__(self, m, k):
        """
        m, size of the vector
        k, number of hash fnctions to compute
        """
        self.m = m
        self.vector = [0]*m
        self.k = k

    def insert(self, key):
        for i in range(self.k):
            self.vector[hash(key+str(i)) % self.m] = 1

    def contains(self, key):
        for i in range(self.k):
            if self.vector[hash(key+str(i)) % self.m] == 0:
                return False
        return True

    def list_to_bloom(self, keys):
        for key in keys:
            self.insert(key)
