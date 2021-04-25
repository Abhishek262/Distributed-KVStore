from kvcache import KVCache
from kvstore import KVStore
from kvconstants import ErrorCodes
from kvmessage import KVMessage
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
        self.state = ErrorCodes.TPCStates["TPC_READY"]
        self.useTPC = useTPC
        #self.TPClog = TPClog(dirname)
        #self.message = message
        #self.phase = phase 

    def KVServerGet(self,key) : 
        lock = self.cache.KVCacheGetLock(key)
        if lock == None:
            return ErrorCodes.errkeylen
        lock.acquire()
        val = self.cache.KVCacheGet(key)
        if val[0] >= 0:
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

        if(succ < 0):
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

        if(ret < 0):
            return ret  
        
        lock.acquire()
        self.cache.KVCacheDelete(key)
        lock.release() 

        return 1 

    def KVServerGetInfoMessage(self):
        localtime = time.asctime( time.localtime(time.time()) )
        return localtime +" "+ self.hostname +" "+ self.port
    
    def KVServerHandleNoTPC(self, reqmsg):
        error = -1
        respmsg = KVMessage()
        default = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
        if(reqmsg.key == ""):
            if(reqmsg.msgType == ErrorCodes.kvMessageType["GETREQ"] or reqmsg.msgType == ErrorCodes.kvMessageType["PUTREQ"] or reqmsg.msgType == ErrorCodes.kvMessageType["DELREQ"]):
                respmsg.msgType = "RESP"
                respmsg.message = default
        
        elif(reqmsg.value == "" and reqmsg.msgType ==  ErrorCodes.kvMessageType["PUTREQ"]):
            respmsg.msgType = "RESP"
            respmsg.message = default

        #if(self.state == ErrorCodes.TPCStates["TPC_READY"]):

        elif(reqmsg.msgType == ErrorCodes.kvMessageType["GETREQ"]):
            ret = self.KVServerGet(reqmsg.key)
            if (ret[0] >= 0):
                respmsg.msgType = ErrorCodes.kvMessageType["GETRESP"]
                respmsg.key = reqmsg.key
                respmsg.value = ret[1]
            else:
                respmsg.msgType = "RESP"
                respmsg.message = ErrorCodes.getErrorMessage(ret[0])
            
        elif(reqmsg.msgType == ErrorCodes.kvMessageType["PUTREQ"]):
            ret = self.KVServerPut(reqmsg.key)
            if (ret >= 0):
                respmsg.msgType = "RESP"
                respmsg.value = ErrorCodes.Successmsg
            else:
                respmsg.msgType = "RESP"
                respmsg.message = ErrorCodes.getErrorMessage(ret)

        elif(reqmsg.msgType == ErrorCodes.kvMessageType["DELREQ"]):
            ret = self.KVServerDelete(reqmsg.key)
            if (ret >= 0):
                respmsg.msgType = "RESP"
                respmsg.value = ErrorCodes.Success
            else:
                respmsg.msgType = "RESP"
                respmsg.message = ErrorCodes.getErrorMessage(ret)
            
        elif(reqmsg.msgType == ErrorCodes.kvMessageType["INFO"]):
            respmsg.msgType = ErrorCodes.kvMessageType["INFO"]
            respmsg.message = self.KVServerGetInfoMessage()
        else:
            respmsg.msgType = "RESP" 
            respmsg.message = ErrorCodes.notImplementedMessage
        
        return respmsg

    def KVServerHandle(self, sock_obj):  
        messageobj = KVMessage()  
        reqmsg = messageobj.KVMessageParse(sock_obj)
        #Maybe Error Handling maybe null
        if self.useTPC is False:
            self.KVServerHandleNoTPC(reqmsg)
        '''
        else:
            self.KVServerHandleTPC(reqmsg)
        '''
        messageobj.kvMessageSend(sock_obj)

