import json, socket
import struct


# Used for KVMessage types and messages
GET_REQ = 0
PUT_REQ = 1
DEL_REQ = 2
GET_RESP = 3
RESP = 4
INFO = 11

# Default timeout (in seconds)
TIMEOUT = 3

# Messages for Exceptions thrown by the client
ERRORS = {
    "could_not_connect": "Network Error: Could not connect",
    "could_not_init": "Syntax Error: Your client could not be initialized",
    "generic": "Server Error: Your request could not be processed",
    "invalid_format": "JSON Error: Message format incorrect",
    "invalid_key": "Data Error: Null or empty key",
    "invalid_value": "Data Error: Null or empty value",
    "no_data": "Network Error: Could not receive data",
    "no_such_key": "Data Error: Key does not exist",
    "oversized_key": "Data Error: Oversized key",
    "oversized_value": "Data Error: Oversized value",
    "timeout": "Network Error: Socket timeout",
}



class KVClient:


    def __init__(self, server, port):
        if type(server) != str or type(port) != int or not (0 < port <= 65535):
            raise Exception(ERRORS["could_not_init"])

        self.host_server = server
        self.host_port = port

    def _connect(self):

        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.connect((self.host_server, self.host_port))
        except Exception:
            raise Exception(ERRORS["could_not_connect"])

    def _listen(self, timeout=None):

        self._sock.settimeout(timeout)

        try:
            unpacker = struct.Struct('L')
            size = socket.ntohl(unpacker.unpack(self._sock.recv(4))[0])
            data = self._sock.recv(size)
        except Exception as e:
            raise e
        if not data:
            raise Exception(ERRORS["no_data"])

        return KVMessage(json_data=data)

    def _disconnect(self):

        self._sock.close()

    def info(self):
        return self._send_request(INFO, "", "")

    def put(self, key, value):

        self._check_key(key)
        self._check_value(value)
        return self._send_request(PUT_REQ, key, value)

    def get(self, key):

        self._check_key(key)
        return self._send_request(GET_REQ, key)

    def delete(self, key):

        self._check_key(key)
        return self._send_request(DEL_REQ, key)

    def _send_request(self, req_type, key, value=None):
        #Helper function for sending the three different types of request.
        
        message = KVMessage(msg_type=req_type, key=key, value=value)
        self._connect()
        message.send(self._sock)
        response = self._listen()
        self._disconnect()

        if response.type == GET_RESP:
            return response.value
        elif req_type == INFO:
            return response.message
        elif response.type != RESP:
            raise Exception(ERRORS["generic"])
        elif response.message != "SUCCESS":
            raise Exception(response.message)
        return response.message


    def _check_key(self, key):

        if not key:
            raise Exception(ERRORS["invalid_key"])
        if len(key) > 256:
            raise Exception(ERRORS["oversized_key"])

    def _check_value(self, value):

        if not value:
            raise Exception(ERRORS["invalid_value"])
        if len(value) > (256*1024):
            raise Exception(ERRORS["oversized_value"])


class KVMessage:
    """
    This class is used to to convert the raw data in requests and responses
    to and from JSON.
    """

    def __init__(self, msg_type=None, key=None, value=None, \
                 msg=None, json_data=None):

        if json_data:
            self._from_json(json_data)
        else:
            self.type = msg_type
            self.key = key
            self.value = value
            self.message = msg

    def __str__(self):
        return self._to_json()

    def _from_json(self, data):

        try:
            decoded = json.loads(data)
        except ValueError:
            raise Exception(ERRORS["invalid_format"])
        if "type" not in decoded:
            raise Exception(ERRORS["invalid_format"])
        self.type = decoded["type"]
        if "key" in decoded:
            self.key = decoded["key"]
        if "value" in decoded:
            self.value = decoded["value"]
        if "message" in decoded:
            self.message = decoded["message"]

    def _to_json(self):

        d = {"type": self.type}

        if self.key:
            d["key"] = self.key
        if self.value:
            d["value"] = self.value
        if self.message:
            d["message"] = self.message

        return json.dumps(d)

    def send(self, sock):

        msg_json = self._to_json()
        size = len(msg_json)
        packer = struct.Struct('I')
        packed_data = packer.pack(socket.htonl(size))
        sock.sendall(packed_data)
        sock.sendall(self._to_json())
