#TPC master is a master server which will communicate with slave servers
#It will handle TPC for transactions
# 
# Servers need to register at TPC server before it can handle any client requests

from kvcache import KVCache
from kvmessage import KVMessage
from kvconstants import ErrorCodes
from pyhash import murmur2_x64_64a
from kvconstants import ErrorCodes
from socket_server import connectTo
import threading


def retHash(slave):
    return slave.id

class TPCSlave:
    def __init__(self):
        self.id = 0
        self.host = ''
        self.port = 0

class TPCMaster:

    timeoutSeconds = 2

    def __init__(self, slaveCapacity, redundancy, numCacheSets, cacheSetSize):

        self.cache = KVCache(numCacheSets, cacheSetSize)
        #locking for cache
        self.slaveCount = 0 #active slaves
        self.slaveCapacity = slaveCapacity
        if redundancy < slaveCapacity:
            self.redundancy = redundancy
        else:
            self.redundancy = slaveCapacity
        
        #list of slaves
        self.slaves = []

        #self.client_req = KVMessage()
        #self.errMsg = ""
        self.slaveLock = threading.lock()

        self.sorted = False
        self.state = ErrorCodes.TPCStates["TPC_INIT"]
        self.errorMsg = ""
        self.hasher = murmur2_x64_64a(seed = 32)

    def tpcMasterRegister(self,reqmsg):

        respmsg = KVMessage()
        respmsg.msgType = ErrorCodes.KVMessageType["RESP"]

        if(reqmsg.value == None or reqmsg.key == None or reqmsg.value=="" ):
            respmsg.message = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
            return respmsg

        origState = self.state
        failure = True
        port = reqmsg.value
        hostname = reqmsg.key 
        portlen = len(port)
        hostname = len(hostname)
        formatString = port + ":" + hostname
        hashval = self.hasher(formatString)

        self.slaveLock.acquire()

        for slave in self.slaves : 
            if(slave.id == hashval):
                failure = False 
                self.slaveLock.release()
                self.state = origState
                respmsg = None
                return respmsg

        if(self.slaveCount == self.slaveCapacity):
            self.slaveLock.release()
            self.state = origState
            respmsg = None
            return respmsg 
        else:
            self.slaveCount+=1
            self.updateCheckMasterState() 
      
        slave = TPCSlave()
        slave.id = hashval
        slave.host = hostname 
        slave.port = int(port)
        self.slaves.append(slave)
        self.slaves.sort(key = retHash)
        respmsg.message = ErrorCodes.Successmsg
        self.slaveLock.release()
        return respmsg

    def tpcMasterGetPrimary(self,key):

        self.slaveLock.acquire()
        self.slaves.sort(key = retHash)

        hashval = self.hasher(key)

        for slave in self.slaves:
            if(hashval < slave.id):
                self.slaveLock.release()
                return slave 
            
        self.slaveLock.release()
        return slave[0]
    
    def tpcMasterGetSuccessor(self,predecessor):
        self.slaveLock.acquire()
        self.slaves.sort(key = retHash)

        for i in range(0,len(self.slaves)):
            if(self.slaves[i].id==predecessor.id):
                self.slaveLock.release()

                if(i==len(self.slaves)-1):
                    return self.slaves[0]

                return self.slaves[i+1]

    def tpcMasterHandleGet(self,reqmsg):

        respmsg = KVMessage()
        respmsg.msgType = ErrorCodes.KVMessageType["RESP"]

        if(reqmsg.value == None or reqmsg.key == None or reqmsg.value=="" ):
            respmsg.message = ErrorCodes.getErrorMessage(ErrorCodes.InvalidRequest)
            return respmsg       

        value = ""
        receivedResponse = KVMessage()
        lock = self.cache.KVCacheGetLock(reqmsg.key)
        lock.acquire()
        cacheGet = self.cache.KVCacheGet(reqmsg.key)
        if(cacheGet[0]==1):
            lock.release()
            respmsg.key = reqmsg.key
            respmsg.value = cacheGet[1]
            respmsg.msgType = ErrorCodes.KVMessageType["GETRESP"]

        else:
            lock.release()
            slave = self.tpcMasterGetPrimary(reqmsg.key)
            successfulConnection = False 

            for i in range(0,self.redundancy):
                fd = connectTo(slave.host,slave.port,TPCMaster.timeoutSeconds)
                if(fd == -1):
                    slave = self.tpcMasterGetSuccessor(slave)
                else:
                    successfulConnection = True
                    break

            if(successfulConnection == False):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(-1)
                return respmsg 
            
            respmsg.KVMessageSend(fd)
            receivedResponse.KVMessageParse(fd)
            fd.close()

            if(receivedResponse.msgType == None):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = ErrorCodes.getErrorMessage(-1)
                return respmsg 

            if(receivedResponse.msgType != ErrorCodes.KVMessageType["GETRESP"]):
                respmsg.msgType = ErrorCodes.KVMessageType["RESP"]
                respmsg.message = receivedResponse.message
            else:
                respmsg.key = receivedResponse.key
                respmsg.value = receivedResponse.value
                respmsg.msgType = receivedResponse.msgType
                respmsg.message = ErrorCodes.Successmsg
                lock.acquire()
                self.cache.KVCachePut(respmsg.key,respmsg.value)
                lock.release()
            
        return respmsg
















