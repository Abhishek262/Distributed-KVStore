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

class Server:
    def __init__():

    
    def serverRun(self, hostname, port):
        