from kvcache import KVCache
from kvstore import KVStore

class KVServer : 

    def __init__(self, dirname, numCacheSets, cacheSetSize, maxThreads, hostname, port, useTPC):
        self.KVCache = KVCache(numCacheSets, cacheSetSize) 
        self.KVStore = KVStore(dirname)
        self.hostname = hostname 
        self.port = port 
        self.maxThreads = maxThreads 
        self.listening = True 
        #self.TPClog = TPClog(dirname)
        #self.message = message
        #self.phase = phase

    def get(self,key) : 
        



        