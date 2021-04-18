from kvcache import KVCache
from kvstore import KVStore
from kvconstants import ErrorCodes
import threading
import time

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

    def KVServerGet(self,key) : 
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
        ret = self.cache.KVCachePut(key,val[1])
        lock.release()
        return ret
        #check put() in kvcacheset

    def KVServerPutCheck(self,key,value):
        return self.store.KVStorePutCheck(key,value)

    def KVServerPut(self,key,value):
        lock = self.cache.KVCacheGetLock(key)
        lock.acquire()
        succ = self.cache.KVCachePut(key,value)

        if(succ<0):
            lock.release()
            return succ 
        
        lock.release()

        ret = self.store.KVStorePut(key,value)
        return ret 

    def KVServerDeleteCheck(self,key):
        return self.store.KVStoreDelCheck(key)

    def KVServerDelete(self,key):
        lock = self.cache.KVCacheGetLock(key)
        if(lock <0):
            return ErrorCodes.errkeylen

        ret  = self.store.KVStoreDelete()

        if(ret <0):
            return ret 
        
        lock.acquire()
        self.cache.KVCacheDelete(key)
        lock.release() 

        return 1 

    def KVServerGetInfoMessage(self):
        return time.localtime() +" "+ self.hostname +" "+ self.port
        










        



        