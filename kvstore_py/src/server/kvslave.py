from socket_server import Server, connectTo, serverRunHelper
import sys
from kvserver import KVServer
import argparse

usage  = "sage: kvslave [-t] [--tpc] [slave_port (default=9000)] [master_port (default=8888)]"

tpcMode = 0
slavePort = 9000
masterPort  = 888

slaveHostName = "localhost"
masterHostName = "localhost"

parser = argparse.ArgumentParser(description='Process some integers.')

parser.add_argument("-t")





