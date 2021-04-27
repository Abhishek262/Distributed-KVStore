from socket_server import Server, connectTo, serverRunHelper
import sys
from kvserver import KVServer
import argparse

usage  = "sage: kvslave [-t] [--tpc] [slave_port (default=9000)] [master_port (default=8888)]"

tpcMode = 0
slavePort = 9000
masterPort  = 888
mode = ""

slaveHostName = "localhost"
masterHostName = "localhost"

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument("-t",action="store_true",default=False)
parser.add_argument("slave_port",type=int)
parser.add_argument("master_port",type=int)
args  = parser.parse_args()
# print(args)

tpcMode = args.t 
slavePort = args.slave_port
masterPort = args.master_port
print(tpcMode,slavePort,masterPort)

if(tpcMode):
    mode = "(tpc)"

if(tpcMode):
    print("Slave server",mode," started on",slavePort," listening for master at ", masterHostName, " :",masterPort ) 
else:
    print("Single Node server started on port ",slavePort)

slaveName = "slave-port" +  str(slavePort)

slave = KVServer(slaveName,4, 4, 2, slaveHostName, slavePort, tpcMode)
server = Server(3) #maxthreads

if(tpcMode):
    sockObj = connectTo(masterHostName, masterPort, 0)
    if( sockObj < 0):
        print(" Error registering slave. Could not connect to master on host", masterHostName, "at port", masterPort)
        exit()
    #kvserver_register_master
    #error handling
    sockObj.close()

server.KVServer = slave
server.serverRun(slaveHostName, slavePort)


