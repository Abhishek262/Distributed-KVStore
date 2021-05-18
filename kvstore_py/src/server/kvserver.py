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
        self.state = ErrorCodes.TPCStates["TPC_INIT"]
        self.useTPC = useTPC
        #self.TPClog = TPClog(dirname)
        self.message = None
        #self.phase = phase 

    #Regsiter server for TPCMaster
    def KVServerRegisterMaster(self,sockObj):
        reqmsg = KVMessage()
        reqmsg.msgType = ErrorCodes.KVMessageType["REGISTER"]
        reqmsg.key = self.hostname
        reqmsg.value = int(self.port)
        reqmsg.KVMessageSend(sockObj)

        respmsg = KVMessage()
        respmsg.KVMessageParse()
    
        if(respmsg.message== None or respmsg.message != ErrorCodes.Successmsg):
            return -1 
        else:
            self.state = ErrorCodes.TPCStates["TPC_READY"]
            return 0


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
        if(type(lock) == int):
            return ErrorCodes.errkeylen

        ret  = self.store.KVStoreDelete(key)

        if(ret < 0):
            return ret  
        
        lock.acquire()
        self.cache.KVCacheDelete(key)
        lock.release() 

        return 1 

    def KVServerGetInfoMessage(self):
        localtime = time.asctime( time.localtime(time.time()) )
        return localtime +" "+ self.hostname +" "+ str(self.port)
    
    def KVServerHandleNoTPC(self, reqmsg):
        error = -1
        respmsg = KVMessage()
        default = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
        if(reqmsg.key == ""):
            if(reqmsg.msgType == ErrorCodes.KVMessageType["GETREQ"] or reqmsg.msgType == ErrorCodes.KVMessageType["PUTREQ"] or reqmsg.msgType == ErrorCodes.KVMessageType["DELREQ"]):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = default
            elif(reqmsg.msgType == ErrorCodes.KVMessageType["INFO"]):
                respmsg.msgType = ErrorCodes.KVMessageType["INFO"]
                respmsg.message = self.KVServerGetInfoMessage()
        
        elif(reqmsg.value == "" and reqmsg.msgType ==  ErrorCodes.KVMessageType["PUTREQ"]):
            respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
            respmsg.message = default

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["GETREQ"]):
            ret = self.KVServerGet(reqmsg.key)
            if (ret[0] >= 0):
                respmsg.msgType = ErrorCodes.KVMessageType["GETRESP"]
                respmsg.key = reqmsg.key
                respmsg.value = ret[1]
            else:
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(ret[0])
            
        elif(reqmsg.msgType == ErrorCodes.KVMessageType["PUTREQ"]):
            ret = self.KVServerPut(reqmsg.key,reqmsg.value)
            if (ret >= 0):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.Successmsg
            else:
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(ret)

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["DELREQ"]):
            ret = self.KVServerDelete(reqmsg.key)
            if (ret >= 0):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.Successmsg
            else:
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(ret)
            
        else:
            respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
            respmsg.message = ErrorCodes.notImplementedMessage
        
        return respmsg

    #  Handles an incoming kvmessage REQMSG, and populates the appropriate fields
    #  of RESPMSG as a response. RESPMSG and REQMSG both must point to valid
    #  kvmessage_t structs. Assumes that the request should be handled as a TPC
    #  message. This should also log enough information in the server's TPC log to
    #  be able to recreate the current state of the server upon recovering from
    #  failure. See the spec for details on logic and error messages.
    def KVServerHandleTPC(self,reqmsg):
        respmsg = KVMessage()
        default = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
        if(reqmsg.key == ""):
            if(reqmsg.msgType == ErrorCodes.KVMessageType["GETREQ"] or reqmsg.msgType == ErrorCodes.KVMessageType["PUTREQ"] or reqmsg.msgType == ErrorCodes.KVMessageType["DELREQ"]):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = default
            elif(reqmsg.msgType == ErrorCodes.KVMessageType["INFO"]):
                respmsg.msgType = ErrorCodes.KVMessageType["INFO"]
                respmsg.message = self.KVServerGetInfoMessage()
        
        elif(reqmsg.value == "" and reqmsg.msgType ==  ErrorCodes.KVMessageType["PUTREQ"]):
            respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
            respmsg.message = default

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["GETREQ"]):
            ret = self.KVServerGet(reqmsg.key)
            if (ret[0] >= 0):
                respmsg.msgType = ErrorCodes.KVMessageType["GETRESP"]
                respmsg.key = reqmsg.key
                respmsg.value = ret[1]
            else:
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(ret[0])

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["PUTREQ"]):
            if(self.state == ErrorCodes.TPCStates["TPC_READY"]):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = default

            #tpclog()
            if(self.KVServerPutCheck(reqmsg.key,reqmsg.value)==1):
                self.copyAndStoreKVMessage(reqmsg)
                respmsg.msgType = ErrorCodes.KVMessageType["VOTE_COMMIT"]
            
            else:
                self.state = ErrorCodes.TPCStates["TPC_INIT"]
                respmsg.msgType = ErrorCodes.KVMessageType["VOTE_ABORT"]
                respmsg.message = ErrorCodes.getErrorMessage(0)

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["DELREQ"]):
            if(self.state == ErrorCodes.TPCStates["TPC_READY"]):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = default

            #tpclog()
            if(self.KVServerDeleteCheck(reqmsg.key,reqmsg.value)==1):
                self.copyAndStoreKVMessage(reqmsg)
                respmsg.msgType = ErrorCodes.KVMessageType["VOTE_COMMIT"]
            
            else:
                self.state = ErrorCodes.TPCStates["TPC_INIT"]
                respmsg.msgType = ErrorCodes.KVMessageType["VOTE_ABORT"]
                respmsg.message = ErrorCodes.getErrorMessage(0)

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["COMMIT"]):
            self.state = ErrorCodes.TPCStates["TPC_READY"]
            #tpclog()

            if(self.message.msgType == ErrorCodes.KVMessageType["PUTREQ"]):
                ret = self.KVServerPut(reqmsg.key,reqmsg.value)
                if(ret<0):
                    respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                    respmsg.message = default
                else:
                    respmsg.msgType = ErrorCodes.KVMessageType["ACK"]

            if(self.message.msgType == ErrorCodes.KVMessageType["DELREQ"]):
                ret = self.KVServerDelete(reqmsg.key,reqmsg.value)
                if(ret<0):
                    respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                    respmsg.message = default
                else:
                    respmsg.msgType = ErrorCodes.KVMessageType["ACK"]      

        elif(reqmsg.msgType == ErrorCodes.KVMessageType["ABORT"]):
            self.state = ErrorCodes.TPCStates["TPC_READY"]
            #tpclog()
            respmsg.msgType = ErrorCodes.KVMessageType["ACK"]

        else:
            respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
            respmsg.message = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
        
        return respmsg
        
    def copyAndStoreKVMessage(self,msg):
        self.message = KVMessage()

        if(msg.key!=None or msg.key!= ""):
            self.message.key = msg.key
        else : 
            self.message.key = ""
            
        if(msg.value!=None or msg.value!=""):
            self.message.value = msg.value 
        else : 
            self.message.value = ""
        
        self.message.msgType = msg.msgType 

    def KVServerHandle(self, sock_obj):  
        messageobj = KVMessage()  
        messageobj.KVMessageParse(sock_obj)
        # print(reqmsg)
        #Maybe Error Handling maybe null
        if self.useTPC is False:
            respmsg = self.KVServerHandleNoTPC(messageobj)
        
        else:
            self.KVServerHandleTPC(messageobj)
        
        respmsg.KVMessageSend(sock_obj)

    def KVServerClean(self):
        return self.store.KVStoreClean()

