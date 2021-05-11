class ErrorCodes:
    errkeylen = -11
    errvallen = -10 
    keyNotFound = -42
    fileError = -12
    InvalidRequest = -1
    Successmsg = "SUCCESS"
    notImplementedMessage = "ERROR: NOT IMPLMENTED"


    KVMessageType = {
        "GETREQ": 0,
        "PUTREQ": 1,
        "DELREQ": 2,
        "GETRESP": 3,
        "RESP": 4,
        "ACK": 5,
        "ABORT": 6,
        "COMMIT": 7,
        "VOTE_COMMIT": 8,
        "VOTE_ABORT": 9,
        "REGISTER": 10,
        "INFO": 11 
    }

    TPCStates = {
        "TPC_INIT": 0,
        "TPC_WAIT": 1,
        "TPC_READY": 2,
        "TPC_ABORT": 3,
        "TPC_COMMIT": 4
    }

    @staticmethod
    def getErrorMessage(error):
        if(error == ErrorCodes.errkeylen):
            return "Error: Improper Key Length"
        if(error == ErrorCodes.errvallen):
            return "Error: Value Too Long"
        if(error == ErrorCodes.keyNotFound):
            return "Error: Key Not Found"
        if(error == ErrorCodes.InvalidRequest):
            return "Error: Invalid Request"       
        return "Error: Unable To Process Request"