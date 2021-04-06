import threading
lock = threading.lock()

class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = Node
        self.next = None
    

class LRUcache:
    cache_limit = None

    def __init__(self, cacheSize):
        if cacheSize <= 0:
            raise ValueError("Size <= 0")
        self.hashmap = dict()
        self.head = None
        self.tail = None
        self.size = cacheSize
        self.curr_size = 0
    
    def get(self, key):
        if key not in self.hashmap:
            return -1
        node = self.hashmap[key]
        if self.head == node:
            return node.value
        updateLRUOrder(node)
        return node.Value

        
    def put(self, key, value):

        lock.acquire()
        if key in hashmap:
            node = slef.hashmap[key]
            node.value = value
            if self.head != node:
                updateLRUOrder(node)
        else:
            new_node = Node(key, value)
            if self.curr_size == self.size:
                remove(self.tail)
            self.setHead(new_node)
            self.hashmap[key] = value
        lock.release()

    def LRUOrder(self, node):
        lock.acquire()
        self.remove(node)
        self.setHead(node)
        lock.release()

    def remove(self, node):
        if not self.head:   
            return

        if node.prev:
            node.prev.next = node.next

        if node.next:
            node.next.prev = node.prev
        
        if (not node.prev) and (not node.next):
            self.head = None
            self.tail = None

        if self.end == node:
            self.end = node.prev
            self.end.next = None
        self.curr_size -= 1
        return node

    def setHead(self, node):
        if not self.head:
            self.head = node
            send.end = node
        
        else:   
            node.next = self.head
            self.head.prev = node
            self.head = node

        self.curr_size += 1 
        

    def del(self, key):
        #Need to be completed