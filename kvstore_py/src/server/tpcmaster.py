#TPC master is a master server which will communicate with slave servers
#It will handle TPC for transactions
# 
# Servers need to register at TPC server before it can handle any client requests

from kvcache import KVCache
from kvmessage import KVMessage
from kvconstants import ErrorCodes

class TPCSlave:
    def __init__(self):
        self.id = 0
        self.host = ''
        self.port = 0

class TPCMaster:

    def __init__(self, slaveCapacity, redundancy, numCacheSets, cacheSetSize):

        self.cache = KVCache(numCacheSets, cacheSetSize)
        #locking for cache
        self.slaveCount = 0 #active slaves
        self.slaveCapacity = slaveCapacity
        if redundancy < slaveCapacity:
            self.redundancy = redundancy
        else:
            self.redundancy = slaveCapacity
        
        self.slaves = []
        self.sorted = False
        self.state = ErrorCodes.TPCStates["TPC_INIT"]
        self.errorMsg = ""
    
    