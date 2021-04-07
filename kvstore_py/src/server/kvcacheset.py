
import threading
lock = threading.Lock()

class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.prev = None
        self.next = None
    

class KVCacheSet:
    cache_limit = None

    def __init__(self, cacheSize):
        if cacheSize <= 0:
            raise ValueError("Size <= 0")
        self.hashmap = dict()
        self.head = None
        self.tail = None
        self.size = cacheSize
        self.curr_size = 0

    def updateLRUOrder(self, node):
        lock.acquire()
        self.remove(node)
        self.setHead(node)
        lock.release()  
        

    def get(self, key):
        # print("IN")
        if key not in self.hashmap:
            print("sf")
            return -1
        node = self.hashmap[key]
        # print(node)
        # print(self.head)
        # for kd,vd in self.hashmap.items():
        #     print(kd,vd,"\n")
        if self.head == node:
            return node.value
        self.updateLRUOrder(node)
        return node.value

        
    def put(self, key, value):

        lock.acquire()
        if key in self.hashmap:
            node = self.hashmap[key]
            node.value = value
            if self.head != node:
                self.updateLRUOrder(node)
        else:
            new_node = Node(key, value)
            if self.curr_size == self.size:
                self.remove(self.tail)
            self.setHead(new_node)
            self.hashmap[key] = new_node
        lock.release()

    def updateLRUOrder(self, node):
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
        return node

    def setHead(self, node):
        if not self.head:
            self.head = node
            self.end = node
        
        else:   
            node.next = self.head
            self.head.prev = node
            self.head = node

        self.curr_size += 1 
        

    def delete(self, key):
        try : 
            node = self.hashmap[key]
            self.remove(node)
            self.hashmap.pop(key)
            self.curr_size -=1
            return 0

        except:
            return -1

    def printCacheElements(self):
        for kh,vh in self.hashmap.items():
            print("K:",kh, " V: ",vh.value)

# a = KVCacheSet(10)
# a.put("ad",10)
# print(a.get("ad"))
# a.printCacheElements()