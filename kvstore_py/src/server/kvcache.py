from kvcacheset import KVCacheSet
from kvstore import hash

class KVCache : 
    maxKeyLength = 128
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
        return cacheSet.get(key)


    def KVCachePut(self,key,value): 
        cacheSet = self.getCacheSet(key)
        cacheSet.put(key,value)
        

    def KVCacheDelete(self,key):
        cacheSet = self.getCacheSet(key)
        cacheSet.delete(key)

    def KVCacheGetLock(self,key):
        if(len(key) > KVCache.maxKeyLength):
            return None
        return self.getCacheSet(key).lock

    #debug stuff
    def __str__(self):
        for x in self.cacheSets:
            print(x)

'''   
a = KVCache(2,1)
a.KVCachePut("ad",10)
b = a.KVCacheGet("ad")
print(b)
c = a.KVCacheGetLock("ad")
print(c)
'''