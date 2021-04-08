'''
typedef struct {
  msgtype_t type;    /* The type of this message. */
  char *key;         /* The key this message stores. May be NULL, depending on type. */
  char *value;       /* The value this message stores. May be NULL, depending on type. */
  char *message;     /* The message this message stores. May be NULL, depending on type. */
} kvmessage_t;
'''

import json
import struct

class KVMessage:

    def __init__(self, msgType, paramList = {}):
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
            ret = sock.recv(size - len(buf))
            if not ret:
                raise Exception("Socket Closed")
            buf += ret
        return buf

    def kvMessageParse(self, sock_obj): #not sure but s here is socket object
        datasize = struct.calcsize("L")
        size = struct.unpack("L", readblob(sock_obj, datsize))
        data = readblob(sock_obj, size)
        temp_dict = json.loads(data.decode('utf-8'))
        
        if "key" in temp_dict.keys():
            self.key = temp_dict["key"]
        
        if "value" in temp_dict.keys():
            self.value = temp_dict["value"]

        if "type" in temp_dict.keys():
            self.msgType = temp_dict["type"]

        if "message" in temp_dict.keys():
            self.message = temp_dict["message"]

        
    def kvMessageSend(self, sock_obj):
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
        
        
        jdata = json.dumps(temp_dict.encode('utf-8'))
        sock_obj.sendall(struct.pack("L", len(jdata)))
        sock_obj.sendall(jdata)
        return datasize+len(jdata)

