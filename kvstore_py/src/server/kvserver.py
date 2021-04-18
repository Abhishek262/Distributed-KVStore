from kvcache import KVCache
from kvstore import KVStore
from kvconstants import ErrorCodes
import threading
class KVServer : 

    def __init__(self, dirname, numCacheSets, cacheSetSize, maxThreads, hostname, port, useTPC):
        self.cache = KVCache(numCacheSets, cacheSetSize) 
        self.store = KVStore(dirname)
        self.hostname = hostname 
        self.port = port 
        self.maxThreads = maxThreads 
        self.listening = True 
        #self.TPClog = TPClog(dirname)
        #self.message = message
        #self.phase = phase 

    def get(self,key) : 
        lock = self.cache.KVCacheGetLock(key)
        if lock == None:
            return ErrorCodes.errkeylen
        lock.acquire()
        val = self.cache.KVCacheGet(key)
        if val is not None:
            lock.release()
            return val 
        lock.release()

        val = self.store.KVStoreGet(key)
        if val[0]<0 :
            return val
        lock.acquire()
        self.cache.KVCachePut(key,val[1])
        lock.release()
        #check put() in kvcacheset

        



        