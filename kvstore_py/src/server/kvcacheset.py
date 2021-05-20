
import threading

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
        self.end = None
        self.size = cacheSize
        self.curr_size = 0
        self.lock = threading.Lock()


    def get(self, key):
        #Locking done by slave server or tpc master
        if key not in self.hashmap:
            #print("sf")
            return None
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

        # self.lock.acquire()
        if key in self.hashmap:
            node = self.hashmap[key]
            node.value = value
            if self.head != node:
                self.updateLRUOrder(node)
        else:
            new_node = Node(key, value)
            if self.curr_size == self.size:
                self.remove(self.end)
            self.setHead(new_node)
            self.hashmap[key] = new_node
        # self.lock.release()
        

    def updateLRUOrder(self, node):
        # self.lock.acquire()
        self.remove(node)
        self.setHead(node)
        # self.lock.release()

    def remove(self, node):
        if node == None:
            return
        if not self.head:   
            return

        if node.prev:
            node.prev.next = node.next

        if node.next:
            node.next.prev = node.prev
        
        if (not node.prev) and (not node.next):
            self.head = None
            self.end = None

        if self.end == node:
            self.end = node.prev
            if self.end:
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
# print(a.delete("ad"))
# print(a.get("ad"))
# a.printCacheElements()