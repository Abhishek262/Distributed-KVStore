from kvcacheset import KVCacheSet
from kvstore import hash

class KVCache : 
    def __init__(self,numCacheSets,cacheSetSize) : 
        if numCacheSets <= 0 or cacheSetSize <=0 :
            raise ValueError("Size <= 0")

        self.numCacheSets = numCacheSets
        self.cacheSetSize = cacheSetSize
        self.cacheSets = []

        for i in range(numCacheSets):
            tempCacheSet = KVCacheSet(cacheSetSize)
            self.cacheSets.append(tempCacheSet)
        
    # Retrieves the cache set associated with a given KEY. The correct set can be
    # determined based on the hash of the KEY using the hash() function defined
    # within kvstore.py
    def getCacheSet(self,key):
        index  =hash(key) % self.numCacheSets
        return self.cacheSets[index]

    def KVCacheGet(self,key):
        cacheSet = self.getCacheSet(key)
        return cacheSet


    def KVCachePut(self,key,value): 
        cacheSet = self.getCacheSet(key)
        cacheSet.put(key,value)
        

    def KVCacheDelete(self,key):
        cacheSet = self.getCacheSet(key)
        cacheSet.delete(key)


    #debug stuff
    def __str__(self):
        for x in self.cacheSets:
            print(x)
    