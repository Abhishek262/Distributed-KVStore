# int master;               /* 1 if this server represents a TPC Master, else 0. */
# int listening;            /* 1 if this server is currently listening, else 0. */
# int sockfd;               /* The socket fd this server is operating on. */
# int max_threads;          /* The maximum number of concurrent jobs that can run. */
# int port;                 /* The port this server will listen on. */
# char *hostname;           /* The hostname this server will listen on. */
# wq_t wq;                  /* The work queue this server will use to process jobs. */
# union {                   /* The kvserver OR tpcmaster this server represents. */
# kvserver_t kvserver;
# tpcmaster_t tpcmaster;
# };

from wq import WorkQueue
import socket 
import sys
import threading
        
def connectTo(hostname, port, timeout):
    try: 
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        # print ("Socket successfully created")
    except socket.error as err: 
        print ("socket creation failed with error %s" %(err))    

    try: 
        host_ip = socket.gethostbyname(hostname) 
    except socket.gaierror: 
    
        # this means could not resolve the host 
        print ("there was an error resolving the host")
        sys.exit()  

    if(timeout>0):
        s.settimeout(timeout)

    try : 
        s.connect((host_ip, port)) 
    except : 
        return -1

    return s 


class Server:
    def __init__(self,maxThreads = 3):
        self.KVServer = None 
        self.TPCMaster = None 
        self.maxThreads = maxThreads
    
    def serverRun(self, hostname, port):
        self.wq = WorkQueue()
        self.listening = True
        self.port = port 
        self.hostname = hostname 

        try: 
            self.sockobj = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
            # print ("Socket successfully created")
        except socket.error as err: 
            print ("socket creation failed with error %s" %(err))            
        
        self.sockobj.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sockobj.bind(("",port))
        self.sockobj.listen(1024)  
        
        #handle the thread part
        threads = []
        for i in range(self.maxThreads):
            t = threading.Thread(target=serverRunHelper,args =(self,))
            threads.append(t)
            t.start()
            
        for i in range(len(threads)):
            threads[i].join()

        self.sockobj.shutdown(socket.SHUT_RDWR)
        self.sockobj.close()
        return 0


    def handle(self):
        if(self.KVServer !=None):
            self.handleSlave()
        else:
            self.handleMaster()

    # def handleMaster(self):
    #     pass 

    def handleSlave(self):
        sockObj = self.wq.pop()
        self.KVServer.KVServerHandle(sockObj)

    def serverStop(self):
        self.listening = False
        self.sockobj.shutdown(socket.SHUT_RDWR)
        self.sockobj.close()


def serverRunHelper(serverObj):

    while(serverObj.listening):
        clientSock, client_addr = serverObj.sockobj.accept()
        serverObj.wq.push(clientSock)

        #if(serverObj.KVServer !=None):
        serverObj.handle()
        # else:
        #     serverObj.TPCMaster.handle()
    

