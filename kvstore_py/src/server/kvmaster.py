from socket_server import Server,connectTo, serverRunHelper
from tpcmaster import TPCMaster
import sys

usage = "Usage: kvmaster [port (default=8888)]"
port = 8888
server = Server()

if(len(sys.argv)>1):
    if(len(sys.argv)>2):
        print(usage)
    
    port = int(sys.argv[1])

server.maxThreads = 3
server.TPCMaster = TPCMaster(2,2,4,4)
print("TPC Master server started listening on port ",port)
server.serverRun("localhost",port)

