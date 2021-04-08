from queue import Queue

class WorkQueue:
    def __init__(self):
        self.queue = Queue()
        #print(self.queue.qsize())
    
    def push(self, item):
        self.queue.put(item)
    
    def pop(self):
        new_item = self.queue.get()
        return new_item
b = WorkQueue()
b.push("a")
print(b.pop())
#print(b.pop())
