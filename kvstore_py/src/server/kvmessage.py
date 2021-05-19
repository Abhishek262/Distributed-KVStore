import json, socket
import struct

class KVMessage:

    def __init__(self, msgType = -1, paramList = {}):
        self.msgType = msgType
        if "message" in paramList.keys():
            self.message = paramList["message"]
        else:
            self.message = ""
        if "key" in paramList.keys():
            self.key = paramList["key"]
        else:
            self.key = ""
        if "value" in paramList.keys():
            self.value = paramList["value"]
        else:
            self.value = ""

    def readblob(self, sock_obj, size):
        buf = ""
        while len(buf) != size:
            ret = sock_obj.recv(size)
            # print(ret)
            if not ret:
                raise Exception("Socket Closed")
            buf += ret
        return buf

    def readBin(self, sock_obj, size):
        # print("started")
        ret = sock_obj.recv(size)
        # print("end")

        if not ret:
            raise Exception("Socket Closed")
        return ret

    def KVMessageParse(self, sock_obj): #not sure but s here is socket object
        datasize = struct.calcsize("L")
        size = struct.unpack("L", self.readBin(sock_obj, datasize))
        size = socket.ntohl(size[0])
        # print("Parse",size)
        data = self.readBin(sock_obj, size)
        # print("data : ",data)
        temp_dict = json.loads(data.decode('utf-8'))
        # print(temp_dict)

        if "key" in temp_dict.keys():
            self.key = temp_dict["key"]
        
        if "value" in temp_dict.keys():
            self.value = temp_dict["value"]

        if "type" in temp_dict.keys():
            self.msgType = temp_dict["type"]

        if "message" in temp_dict.keys():
            self.message = temp_dict["message"]
 
        
    def KVMessageSend(self, sock_obj):
        sent = 0
        temp_dict = dict()
        temp_dict["type"] = self.msgType
        datasize = struct.calcsize("L")

        if self.key != "":
            temp_dict["key"] = self.key

        if self.value != "":
            temp_dict["value"] = self.value
        
        if self.message != "":
            temp_dict["message"] = self.message
        
        '''if "key" in temp_dict.keys():
            temp_dict["key"] = self.key
        if "value" in temp_dict.keys():
            temp_dict["value"] = self.value
        if "message" in temp_dict.keys():
            temp_dict["message"] = self.message'''
        
        
        jdata = json.dumps(temp_dict).encode('utf-8')
        size = socket.htonl(len(jdata))
        sock_obj.sendall(struct.pack("L", size))
        sock_obj.sendall(jdata)
        return datasize+len(jdata)

